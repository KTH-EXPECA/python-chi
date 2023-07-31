import json, time
from loguru import logger
import chi
from chi import lease
from blazarclient.exception import BlazarClientException


LEASE_STATUS_CHECK_TIMEOUT_SEC = 120
LEASE_STATUS_CHECK_PERIOD_SEC = 5
CREATE_LEASE_RETRY_NUM = 2
CREATE_LEASE_RETRY_PERIOD_SEC = 5
REMOVE_LEASE_RETRY_NUM = 1
REMOVE_LEASE_RETRY_PERIOD_SEC = 5


# NOTE: do not push too many lease requests at the same time it causes errors
# wait until the previous request is handled then send the next one

def get_lease_status(leaseid: str) -> str:
    leaselist = chi.blazar().lease.list()
    status = None
    for lease in leaselist:
        if lease['id'] == leaseid:
            status = lease['status']
    return status

def wait_until_lease_status(
        leaseid : str,
        leasename : str,
        desiredstatus : str, 
        erroneouslease : bool = False,
    ) -> None:

    logger.info(f"waiting {LEASE_STATUS_CHECK_TIMEOUT_SEC} seconds"\
                f" for {leasename} with id {leaseid} to become \"{desiredstatus}\"")
    
    mustend = time.time() + LEASE_STATUS_CHECK_TIMEOUT_SEC
    while time.time() < mustend:
        time.sleep(LEASE_STATUS_CHECK_PERIOD_SEC)
        status = get_lease_status(leaseid)
        logger.info(f"lease {leasename} with id {leaseid} is {status}.")
        if status == desiredstatus:
            return
        elif status == "ERROR":
            if not erroneouslease:
                raise BlazarClientException(message="unknown")
    
    raise BlazarClientException(message=f"timeout reached, lease {leasename} with id {leaseid} is stuck in {status}.")

def remove_lease(
        leaseid : str,
        leasename : str,
        erroneouslease : bool = False,
    ) -> None:

    logger.info(f"Removing {leasename} reservation with id {leaseid}.")
    def try_to_remove() -> bool:
        try:
            status = get_lease_status(leaseid)
            if status == None:
                raise BlazarClientException("lease has already been removed.")
            elif status == "STARTING" or status == "DELETING":
                raise BlazarClientException("lease is in {status} state.")
            else:
                chi.blazar().lease.delete(leaseid)
                wait_until_lease_status(leaseid, leasename, None, erroneouslease)
                logger.success("done")
                return True
        except BlazarClientException as ex:
            msg: "str" = ex.args[0]
            msg = msg.lower()
            logger.warning(f"removing {leasename} reservation with id {leaseid} failed. msg: {msg}")
            return False

    retries_left = 1+REMOVE_LEASE_RETRY_NUM
    while retries_left > 0:
        if try_to_remove():
            break
        retries_left=retries_left-1
        if retries_left <= 0:
            logger.error(f"giving up on removing {leasename} reservation with id {leaseid}.")
        else:
            logger.info(f"retrying to force remove {leaseid}. {retries_left} retry(s) left.")
            logger.info(f"waiting {REMOVE_LEASE_RETRY_PERIOD_SEC} seconds"\
                f" for {leasename} with id {leaseid} to retry deleting it.")
            time.sleep(REMOVE_LEASE_RETRY_PERIOD_SEC)


def shorten_lease(lease: dict):
    return {
        'name':lease['name'],
        'id':lease['id'],
        'reservation_id':lease['reservations'][0]['id'],
        'status':lease['status'],
        'end_date':lease['end_date']
    }

def show_reservation_byid(leaseid: str, brief : bool = False) -> dict:
    leaselist = chi.blazar().lease.list()
    result = None
    for lease in leaselist:
        if lease['id'] == leaseid:
            if brief:
                result = shorten_lease(lease)
            else:
                result = lease
    return result

def list_reservations(brief : bool = False) -> list:
    leaselist = chi.blazar().lease.list()
    newleaselist = []
    if brief:
        for lease in leaselist:
            newleaselist.append(shorten_lease(lease))
        return newleaselist
    else:
        return leaselist

def unreserve_byid(leaseid : str):

    result = show_reservation_byid(leaseid)
    if result:
        remove_lease(leaseid, result['name'])
    else:
        logger.error(f"no reservation found with id {leaseid}")

def reserve(item : dict):
    logger.info(f"reserving {item['name']}")

    if item['type'] == 'device':
        reservations = []
        lease.add_device_reservation(reservations, machine_name=item['name'])
    elif item['type'] == 'network':
        # NOTE: segment_id must be a string
        # NOTE: separators argument for json dump is critical
        reservations = [
            {
                "resource_type": "network",
                "network_name": item["net_name"] + "-net",
                "network_description": '',
                "resource_properties": json.dumps(
                    ["==", "$vlan_id", item["segment_id"]], 
                    separators=(',', ':'),
                ),
                "network_properties": '',
            }
        ]
    else:
        logger.error(f"\t{item['name']} reservation failed due to wrong type.")
        return

    start_date, end_date = lease.lease_duration(
        days=item["duration"]["days"],
        hours=item["duration"]["hours"],
    )

    def try_to_create_lease() -> dict:
        leaseid = None
        try: 
            leasename = item["name"] + "-lease"
            leaseans = chi.blazar().lease.create(
                name=leasename,
                start=start_date,
                end=end_date,
                reservations=reservations,
                events=[],
            )
            leaseid = leaseans['id']
            wait_until_lease_status(leaseid, leasename, "ACTIVE")
            logger.success("done")
            return leaseans
        except BlazarClientException as ex:
            msg: "str" = ex.args[0]
            msg = msg.lower()
            logger.warning(f"{item['name']} reservation failed. msg: {msg}")
            if leaseid:
                remove_lease(leaseid, leasename, True)
            return None

    retries_left = 1+CREATE_LEASE_RETRY_NUM
    while retries_left > 0:
        answer = try_to_create_lease()
        if answer:
            return answer
        retries_left=retries_left-1
        if retries_left <= 0:
            logger.error(f"giving up on reserving {item['name']}.")
        else:
            logger.info(f"retrying to reserve {item['name']}. {retries_left} retry(s) left.")
            logger.info(f"waiting {CREATE_LEASE_RETRY_PERIOD_SEC} seconds"\
                f" for {item['name']} to retry reserving it.")
            time.sleep(CREATE_LEASE_RETRY_PERIOD_SEC)
