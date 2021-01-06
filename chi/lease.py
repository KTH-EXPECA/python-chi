from datetime import datetime, timedelta
import json
import numbers
import sys
import time

from blazarclient.exception import BlazarClientException
from dateutil import tz
from neutronclient.v2_0.client import Client as NeutronClient

from .clients import blazar
from .context import get as get_from_context, session
from .network import get_network_id, PUBLIC_NETWORK
from .server import Server, ServerError
from .util import random_base32

__all__ = [
    'add_node_reservation',
    'add_network_reservation',
    'lease_duration',
]

BLAZAR_TIME_FORMAT = "%Y-%m-%d %H:%M"
NODE_TYPES = {
    "compute_haswell",
    "compute_skylake",
    "compute_haswell_ib",
    "storage",
    "storage_hierarchy",
    "gpu_p100",
    "gpu_p100_nvlink",
    "gpu_k80",
    "gpu_m40",
    "fpga",
    "lowpower_xeon",
    "atom",
    "arm64",
}
DEFAULT_NODE_TYPE = "compute_haswell"
DEFAULT_LEASE_LENGTH = timedelta(days=1)
DEFAULT_NETWORK_RESOURCE_PROPERTIES = ["==", "$physical_network", "physnet1"]


def lease_create_args(
    neutronclient,
    name=None,
    start="now",
    end=None,
    length=None,
    nodes=1,
    node_resource_properties=None,
    fips=0,
    networks=0,
    network_resource_properties=DEFAULT_NETWORK_RESOURCE_PROPERTIES,
):
    """
    Generates the nested object that needs to be sent to the Blazar client
    to create the lease. Provides useful defaults for Chameleon.

    :param str name: name of lease. If ``None``, generates a random name.
    :param str/datetime start: when to start lease as a
        :py:class:`datetime.datetime` object, or if the string ``'now'``,
        starts in about a minute.
    :param length: length of time as a :py:class:`datetime.timedelta` object or
        number of seconds as a number. Defaults to 1 day.
    :param datetime.datetime end: when to end the lease. Provide only this or
        `length`, not both.
    :param int nodes: number of nodes to reserve.
    :param resource_properties: object that is JSON-encoded and sent as the
        ``resource_properties`` value to Blazar. Commonly used to specify
        node types.
    """
    if start == "now":
        start = datetime.now(tz=tz.tzutc()) + timedelta(seconds=70)

    if length is None and end is None:
        length = DEFAULT_LEASE_LENGTH
    elif length is not None and end is not None:
        raise ValueError("provide either 'length' or 'end', not both")

    if end is None:
        if isinstance(length, numbers.Number):
            length = timedelta(seconds=length)
        end = start + length

    reservations = []

    if nodes > 0:
        if node_resource_properties:
            node_resource_properties = json.dumps(node_resource_properties)

        reservations += [
            {
                "resource_type": "physical:host",
                "resource_properties": node_resource_properties or "",
                "hypervisor_properties": "",
                "min": str(nodes),
                "max": str(nodes),
            }
        ]

    if fips > 0:
        reservations += [
            {
                "resource_type": "virtual:floatingip",
                "network_id": get_network_id(PUBLIC_NETWORK),
                "amount": fips,
            }
        ]

    if networks > 0:
        if network_resource_properties:
            network_resource_properties = json.dumps(network_resource_properties)

        reservations += [
            {
                "resource_type": "network",
                "resource_properties": network_resource_properties or "",
                "network_name": f"{prefix}-net{idx}",
            }
            for idx in range(networks)
        ]

    return {
        "name": name,
        "start": start.strftime(BLAZAR_TIME_FORMAT),
        "end": end.strftime(BLAZAR_TIME_FORMAT),
        "reservations": reservations,
        "events": [],
    }


def lease_create_nodetype(*args, **kwargs):
    """
    Wrapper for :py:func:`lease_create_args` that adds the
    ``resource_properties`` payload to specify node type.

    :param str node_type: Node type to filter by, ``compute_haswell``, et al.
    :raises ValueError: if there is no `node_type` named argument.
    """
    try:
        node_type = kwargs.pop("node_type")
    except KeyError:
        raise ValueError("no node_type specified")
    if node_type not in NODE_TYPES:
        print('warning: unknown node_type ("{}")'.format(node_type), file=sys.stderr)
        # raise ValueError('unknown node_type ("{}")'.format(node_type))
    kwargs["node_resource_properties"] = ["==", "$node_type", node_type]
    return lease_create_args(*args, **kwargs)


class Lease(object):
    """
    Creates and manages a lease, optionally with a context manager (``with``).

    .. code-block:: python

        with Lease(session, node_type='compute_haswell') as lease:
            instance = lease.create_server()
            ...

    When using the context manager, on entering it will wait for the lease
    to launch, then on exiting it will delete the lease, which in-turn
    also deletes the instances launched with it.

    :param keystone_session: session object
    :param bool sequester: If the context manager catches that an instance
        failed to start, it will not delete the lease, but rather extend it
        and rename it with the ID of the instance that failed.
    :param bool _no_clean: Don't delete the lease at the end of a context
        manager
    :param kwargs: Parameters passed through to
        :py:func:`lease_create_nodetype` and in turn
        :py:func:`lease_create_args`
    """

    def __init__(self, **kwargs):
        kwargs.setdefault("session", session())

        self.session = kwargs.pop("session")
        self.blazar = BlazarClient(
            "1", service_type="reservation", session=self.session
        )
        self.neutron = NeutronClient(session=self.session)

        self.lease = None

        self._servers = {}

        self._sequester = kwargs.pop("sequester", False)

        kwargs.setdefault("_preexisting", False)
        self._preexisting = kwargs.pop("_preexisting")

        kwargs.setdefault("_no_clean", False)
        self._noclean = kwargs.pop("_no_clean")

        prefix = kwargs.pop("prefix", "")
        rand = random_base32(6)
        self.prefix = f"{prefix}-{rand}" if prefix else rand

        kwargs.setdefault("name", self.prefix)

        if self._preexisting:
            self.id = kwargs["_id"]
            self.refresh()
        else:
            kwargs.setdefault("node_type", DEFAULT_NODE_TYPE)
            self._lease_kwargs = lease_create_nodetype(self.neutron, **kwargs)
            self.lease = self.blazar.lease.create(**self._lease_kwargs)
            self.id = self.lease["id"]

        self.name = self.lease["name"]
        self.reservations = self.lease["reservations"]

    @classmethod
    def from_existing(cls, id):
        """
        Attach to an existing lease by ID. When using in conjunction with the
        context manager, it will *not* delete the lease at the end.
        """
        return cls(_preexisting=True, _id=id)

    def __repr__(self):
        region = self.session.region_name
        return "<{} '{}' on {} ({})>".format(
            self.__class__.__name__, self.name, region, self.id
        )

    def __enter__(self):
        if self.lease is None:
            # don't support reuse in multiple with's.
            raise RuntimeError("Lease context manager not reentrant")
        self.wait()
        return self

    def __exit__(self, exc_type, exc, exc_tb):
        if exc is not None and self._noclean:
            print("Lease existing uncleanly (noclean = True).")
            return

        if isinstance(exc, ServerError) and self._sequester:
            print("Instance failed to start, sequestering lease")
            self.blazar.lease.update(
                lease_id=self.id,
                name="sequester-error-instance-{}".format(exc.server.id),
                prolong_for="6d",
            )
            return

        # if lease exists, delete instances
        current_lease = self.blazar.lease.get(self.id)
        if current_lease:
            for server in self.servers:
                server.delete()

        if not self._preexisting:
            # don't auto-delete pre-existing leases
            self.delete()

    def refresh(self):
        """Updates the lease data"""
        self.lease = self.blazar.lease.get(self.id)

    @property
    def node_reservation(self):
        return next(
            iter(
                [
                    r["id"]
                    for r in (self.reservations or [])
                    if r["resource_type"] == "physical:host"
                ]
            ),
            None,
        )

    @property
    def status(self):
        """Refreshes and returns the status of the lease."""
        self.refresh()
        # NOTE(priteau): Temporary compatibility with old and new lease status
        if self.lease.get("action") is not None:
            return self.lease["action"], self.lease["status"]
        else:
            return self.lease["status"]

    @property
    def ready(self):
        """Returns True if the lease has started."""
        # NOTE(priteau): Temporary compatibility with old and new lease status
        if self.lease.get("action") is not None:
            return self.status == ("START", "COMPLETE")
        else:
            return self.status == "ACTIVE"

    @property
    def servers(self):
        return self._servers.values()

    @property
    def binding(self):
        return {
            key: {
                "address": value.ip,
                "auth": {
                    "user": "cc",
                    "private_key": get_from_context("keypair_private_key"),
                },
            }
            for key, value in self._servers.items()
        }

    def wait(self):
        """Blocks for up to 150 seconds, waiting for the lease to be ready.
        Raises a RuntimeError if it times out."""
        for _ in range(15):
            time.sleep(10)
            if self.ready:
                break
        else:
            raise RuntimeError("timeout, lease failed to start")

    def delete(self):
        """Deletes the lease"""
        self.blazar.lease.delete(self.id)
        self.lease = None

    def create_server(self, *server_args, **server_kwargs):
        """Generates instances using the resource of the lease. Arguments
        are passed to :py:class:`ccmanage.server.Server` and returns same
        object."""
        server_kwargs.setdefault("lease", self)
        server_name = server_kwargs.pop("name", len(self.servers))
        server_kwargs.setdefault("name", f"{self.prefix}-{server_name}")
        server = Server(*server_args, **server_kwargs)
        self._servers[server_name] = server
        return server


def add_node_reservation(reservation_list, count=1, node_type=DEFAULT_NODE_TYPE):
    """Add a node reservation to a reservation list.

    Args:
        reservation_list (list[dict]): The list of reservations to add to.
            The list will be extended in-place.
        count (int): The number of nodes of the given type to request.
            (Default 1).
        node_type (str): The node type to request. (Default "compute_haswell").
    """
    reservation_list.append({
        "resource_type": "physical:host",
        "resource_properties": json.dumps(["==", "$node_type", node_type]),
        "hypervisor_properties": "",
        "min": count,
        "max": count
    })


def add_network_reservation(reservation_list,
                            network_name,
                            of_controller_ip=None,
                            of_controller_port=None,
                            vswitch_name=None,
                            physical_network="physnet1"):
    """Add a network reservation to a reservation list.

    Args:
        reservation_list (list[dict]): The list of reservations to add to.
            The list will be extended in-place.
        network_name (str): The name of the network to create when the
            reservation starts.
        of_controller_ip (str): The OpenFlow controller IP, if the network
            should be controlled by an external controller.
        of_controller_port (int): The OpenFlow controller port.
        vswitch_name (str): The name of the virtual switch associated with
            this network. See `the virtual forwarding context documentation
            <https://chameleoncloud.readthedocs.io/en/latest/technical/networks/networks_sdn.html#corsa-dp2000-virtual-forwarding-contexts-network-layout-and-advanced-features>`_
            for more details.
        physical_network (str): The physical provider network to reserve from.
            This only needs to be changed if you are reserving a `stitchable
            network <https://chameleoncloud.readthedocs.io/en/latest/technical/networks/networks_stitching.html>`_.
            (Default "physnet1").
    """
    desc_parts = []
    if of_controller_ip and of_controller_port:
        desc_parts.append(
            f'OFController={of_controller_ip}:{of_controller_port}')
    if vswitch_name:
        desc_parts.append(f'VSwitchName={vswitch_name}')

    reservation_list.append({
        'resource_type': 'network',
        'network_name': network_name,
        'network_description': ','.join(desc_parts),
        'resource_properties': json.dumps(['==', '$physical_network', physical_network]),
        'network_properties': '',
    })


def add_fip_reservation(reservation_list, count=1):
    reservation_list.append({
        'resource_type': 'virtual:floatingip',
        'network_id': get_network_id(PUBLIC_NETWORK),
        'amount': count
    })


def get_floating_ip_by_reservation_id(reservation_id):
    # blazar().lease.list()
    pass


def lease_duration(days=1, hours=None):
    """Compute the start and end dates for a lease given its desired duration.

    When providing both ``days`` and ``hours``, the duration is summed. So,
    the following would be a lease for one and a half days:

    .. code-block:: python

       start_date, end_date = lease_duration(days=1, hours=12)

    Args:
        days (int): The number of days the lease should be for.
        hours (int): The number of hours the lease should be for.
    """
    now = datetime.now(tz=tz.tzutc())
    # Start one minute into future to avoid Blazar thinking lease is in past
    # due to rounding to closest minute.
    start_date = (now + timedelta(minutes=1)).strftime(BLAZAR_TIME_FORMAT)
    end_date = (now + timedelta(days=days, hours=hours)).strftime(BLAZAR_TIME_FORMAT)
    return start_date, end_date


def reserve_node(lease_name, node_type="compute_haswell", count=1):
    '''
    This is the description of the library function for reserve_node from the api in reservation_api_examples.py.

    Parameters
    ----------
    lease_name : str
        Description of parameter `lease_name`.
    node_type : str
        Description of parameter `node_type`
    count : int
        Description of parameter `count`
    '''
    start_date, end_date = lease_duration(days=1)

    # Build list of reservations (in this case there is only one reservation)
    reservation_list = []
    add_node_reservation(reservation_list, count=count, node_type=node_type)

    # Create the lease
    return blazar().lease.create(name=lease_name,
                                 start=start_date,
                                 end=end_date,
                                 reservations=reservation_list, events=[])


def reserve_network(lease_name,
                    network_name,
                    of_controller_ip=None,
                    of_controller_port=None,
                    vswitch_name=None,
                    physical_network="physnet1"):
    '''
    This is the description of the library function for reserve_network from the api in reservation_api_examples.py.

    Parameters
    ----------
    lease_name : str
        Description of parameter `lease_name`.
    network_name : str
        Description of parameter `network_name`
    of_controller_ip : str
        Description of parameter `of_controller_ip`
    of_controller_port : str
        Description of parameter `of_controller_port`
    vswitch_name: str
        Description of parameter `vswitch_name`
    physical_network: str
        Description of parameter `physical_network`
    '''
    start_date, end_date = lease_duration(days=1)

    # Build list of reservations (in this case there is only one reservation)
    reservation_list = []
    add_network_reservation(reservation_list,
                            network_name=network_name,
                            of_controller_ip=of_controller_ip,
                            of_controller_port=of_controller_port,
                            vswitch_name=vswitch_name,
                            physical_network=physical_network)

    # Create the lease
    return blazar().lease.create(name=lease_name,
                                 start=start_date,
                                 end=end_date,
                                 reservations=reservation_list,
                                 events=[])


def reserve_floating_ip(lease_name, count=1):
    '''
    This is the description of the library function for reserve_floating_ip from the api in reservation_api_examples.py.

    Parameters
    ----------
    lease_name : str
        Description of parameter `lease_name`.
    count : int
        Description of parameter `count`
    '''
    start_date, end_date = lease_duration(days=1)

    # Build list of reservations (in this case there is only one reservation)
    reservation_list = []
    add_fip_reservation(reservation_list, count=count)

    # Create the lease
    return blazar().lease.create(name=lease_name,
                                 start=start_date,
                                 end=end_date,
                                 reservations=reservation_list, events=[])


def reserve_multiple_resources(lease_name):
    start_date, end_date = lease_duration(days=1)

    # Build list of reservations (in this case there is only one reservation)
    reservation_list = []
    add_node_reservation(reservation_list, count=1, node_type="compute_haswell")
    add_network_reservation(reservation_list, network_name=lease_name+"Network")
    add_fip_reservation(reservation_list, count=1)

    # Create the lease
    return blazar().lease.create(name=lease_name,
                                 start=start_date,
                                 end=end_date,
                                 reservations=reservation_list,
                                 events=[])


#########
# Leases
#########

def get_lease_id(lease_name):
    matching = [l for l in blazar().lease.list() if l['name'] == lease_name]
    if not matching:
        raise ValueError(f'No leases found for name {lease_name}')
    elif len(matching) > 1:
        raise ValueError(f'Multiple leases found for name {lease_name}')
    return matching[0]['id']


def get_lease(ref):
    try:
        return show_lease(ref)
    except BlazarClientException as err:
        if err.code == 404:
            return show_lease(get_lease_id(ref))
        else:
            raise


def delete_lease(lease_id):
    blazar().lease.delete(lease_id)
    print(f'Deleted lease with id {lease_id}')


def delete_lease_by_name(lease_name):
    matching = [l for l in blazar().lease.list() if l['name'] == lease_name]
    if not matching:
        raise ValueError(f'No leases found for name {lease_name}')
    elif len(matching) > 1:
        raise ValueError(f'Multiple leases found for name {lease_name}')
    delete_lease(matching[0]['id'])


def show_lease(lease_id):
    return blazar().lease.get(lease_id)
