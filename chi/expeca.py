import json, time, requests, re
from loguru import logger
import chi
from chi import lease
from chi import container
from blazarclient.exception import BlazarClientException


CONTAINER_STATUS_CHECK_TIMEOUT_SEC = 30
CONTAINER_STATUS_CHECK_PERIOD_SEC = 5
LEASE_STATUS_CHECK_TIMEOUT_SEC = 120
LEASE_STATUS_CHECK_PERIOD_SEC = 5
CREATE_LEASE_RETRY_NUM = 2
CREATE_LEASE_RETRY_PERIOD_SEC = 5
REMOVE_LEASE_RETRY_NUM = 1
REMOVE_LEASE_RETRY_PERIOD_SEC = 5


def get_available_publicips():
    url = "http://testbed.expeca.proj.kth.se:56900/"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        available_ips = data.get('available_ips', [])
        return available_ips
    else:
        logger.error(f"Failed to retrieve data, status code: {response.status_code}")
        return []

# NOTE: name format: sdr-xx, adv-xx, or ep5g
def get_segment_ids(radio_name):
    pattern_sdr = re.compile(r'^sdr-\d{2}$')
    pattern_adv = re.compile(r'^adv-\d{2}$')
    pattern_ep5g = re.compile(r'^ep5g$')
    if not (pattern_sdr.match(radio_name) or pattern_adv.match(radio_name) or pattern_ep5g.match(radio_name)):
        logger.error(f"Wrong format, the argument has to be like sdr-xx, adv-xx, or ep5g")
        return {}
        
    url = f"http://testbed.expeca.proj.kth.se:56901/?name={radio_name}"
    response = requests.get(url)
    if response.status_code == 200:
        answer = response.json()
        result = {}
        if pattern_sdr.match(radio_name):
            for key in answer.keys():
                if 'mango' in key:
                    result['rj45'] = answer[key]['segment_id']
                if 'ni' in key:
                    result['sfp'] = answer[key]['segment_id']
        elif pattern_adv.match(radio_name):
            for key in answer.keys():
                if 'adv' in key:
                    result['rj45'] = answer[key]['segment_id']
        elif pattern_ep5g.match(radio_name):
            for key in answer.keys():
                if 'ep5g' in key:
                    result['rj45'] = answer[key]['segment_id']
        return result
    else:
        logger.error(f"Failed to retrieve data, status code: {response.status_code}")
        return {}

# NOTE: name format: sdr-xx, adv-xx, or ep5g
def get_radio_interfaces(radio_name):
    pattern_sdr = re.compile(r'^sdr-\d{2}$')
    pattern_adv = re.compile(r'^adv-\d{2}$')
    pattern_ep5g = re.compile(r'^ep5g$')
    if not (pattern_sdr.match(radio_name) or pattern_adv.match(radio_name) or pattern_ep5g.match(radio_name)):
        logger.error(f"Wrong format, the argument has to be like sdr-xx, adv-xx, or ep5g")
        return {}
        
    url = f"http://testbed.expeca.proj.kth.se:56901/?name={radio_name}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        logger.error(f"Failed to retrieve data, status code: {response.status_code}")
        return {}

# NOTE: name format: worker-xx
def get_worker_interfaces(worker_name):
    pattern_worker = re.compile(r'^worker-\d{2}$')
    if not pattern_worker.match(worker_name):
        logger.error(f"Wrong format, the argument has to be like worker-xx")
        return {}
        
    url = f"http://testbed.expeca.proj.kth.se:56901/?name={worker_name}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        logger.error(f"Failed to retrieve data, status code: {response.status_code}")
        return {}

def get_container_status(containername: str) -> str:
    containerslist = chi.container.list_containers()
    status = None
    for container in containerslist:
        contdict = container.to_dict()
        if contdict['name'] == containername:
            status = contdict['status']
    return status

def wait_until_container_removed(containername : str) -> None:

    logger.info(f"waiting {CONTAINER_STATUS_CHECK_TIMEOUT_SEC} seconds"\
                f" for {containername} container to be removed")
    
    mustend = time.time() + CONTAINER_STATUS_CHECK_TIMEOUT_SEC
    while time.time() < mustend:
        time.sleep(CONTAINER_STATUS_CHECK_PERIOD_SEC)
        status = get_container_status(containername)
        logger.info(f"container {containername} is in {status} state.")
        if status == None:
            return
    
    raise Exception(f"timeout reached, container {containername} is stuck in {status}.")


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

def show_reservation_byname(leasename: str, brief : bool = False) -> dict:
    leaselist = chi.blazar().lease.list()
    result = None
    for lease in leaselist:
        if lease['name'] == leasename:
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


# function to restart any sdr
def restart_sdr(sdr_name : str, sdr_net_id: str, worker_reservation_id: str, worker_net_interface: str):

    container = chi.container.create_container(
        name = "reboot-sdr",
        image = "samiemostafavi/sdr-tools",
        reservation_id = worker_reservation_id,
        nets = [
            { "network" : sdr_net_id },
        ],
        environment = {
            "SERVICE":"reboot",
            "SDR":sdr_name,
            "JSON_PATH":"sdrs.json"
        },
        labels = {
            "networks.1.interface":worker_net_interface,
            "networks.1.ip":f"10.30.1.253/24"
        },
    )
    chi.container.wait_for_active(f"reboot-sdr")
    logger.success(f"created reboot-sdr container.")

    logger.info(f"waiting 2 minutes for the {sdr_name} to reboot.")
    success = False
    for i in range(100):
        time.sleep(1)
        log = chi.container.get_logs("reboot-sdr")
        if "is up again." in log:
            success = True
            break

    if success:
        logger.success(log)
    else:
        logger.warning(log)

    status = get_container_status("reboot-sdr")
    if status:
        chi.container.destroy_container("reboot-sdr")
        wait_until_container_removed("reboot-sdr")


# function to make any sdr mango
def make_sdr_mango(sdr_name : str, sdr_net_id: str, worker_reservation_id: str, worker_net_interface: str):

    container = chi.container.create_container(
        name = "make-sdr-mango",
        image = "samiemostafavi/sdr-tools",
        reservation_id = worker_reservation_id,
        nets = [
            { "network" : sdr_net_id },
        ],
        environment = {
            "SERVICE":"change_design",
            "DESIGN":'mango',
            "SDR":sdr_name,
            "JSON_PATH":"sdrs.json"
        },
        labels = {
            "networks.1.interface":worker_net_interface,
            "networks.1.ip":f"10.30.1.253/24"
        },
    )
    chi.container.wait_for_active(f"make-sdr-mango")
    logger.success(f"created make-sdr-mango container.")

    logger.info(f"waiting 2 minutes for the {sdr_name} to change design.")
    success = False
    for i in range(100):
        time.sleep(1)
        log = chi.container.get_logs("make-sdr-mango")
        if "design has been changed to mango" in log:
            success = True
            break
        
        if "is already set" in log:
            success = True
            break

    if success:
        logger.success(log)
    else:
        logger.warning(log)

    status = get_container_status("make-sdr-mango")
    if status:
        chi.container.destroy_container("make-sdr-mango")
        wait_until_container_removed("make-sdr-mango")


# function to make any sdr ni
def make_sdr_ni(sdr_name : str, sdr_net_id: str, worker_reservation_id: str, worker_net_interface: str):

    container = chi.container.create_container(
        name = "make-sdr-mango",
        image = "samiemostafavi/sdr-tools",
        reservation_id = worker_reservation_id,
        nets = [
            { "network" : sdr_net_id },
        ],
        environment = {
            "SERVICE":"change_design",
            "DESIGN":'ni',
            "SDR":sdr_name,
            "JSON_PATH":"sdrs.json"
        },
        labels = {
            "networks.1.interface":worker_net_interface,
            "networks.1.ip":f"10.30.1.253/24"
        },
    )
    chi.container.wait_for_active(f"make-sdr-mango")
    logger.success(f"created make-sdr-mango container.")

    logger.info(f"waiting 2 minutes for the {sdr_name} to change design.")
    success = False
    for i in range(100):
        time.sleep(1)
        log = chi.container.get_logs("make-sdr-mango")
        if "design has been changed to ni" in log:
            success = True
            break
        
        if "is already set" in log:
            success = True
            break

    if success:
        logger.success(log)
    else:
        logger.warning(log)

    status = get_container_status("make-sdr-mango")
    if status:
        chi.container.destroy_container("make-sdr-mango")
        wait_until_container_removed("make-sdr-mango")

# function to run sdr_tools
def sdr_tools(sdr_name : str, sdr_net_id: str, environment: dict, waiting_iter: int, waiting_sec: int, worker_reservation_id: str, key_str: str, verbose: bool, worker_net_interface: str):
    cont_name = f"{sdr_name}-tools"
    container = chi.container.create_container(
        name = cont_name,
        image = "samiemostafavi/sdr-tools",
        reservation_id = worker_reservation_id,
        nets = [
            { "network" : sdr_net_id },
        ],
        environment = environment,
        labels = {
            "networks.1.interface":worker_net_interface,
            "networks.1.ip":f"10.30.1.253/24"
        },
    )
    chi.container.wait_for_active(cont_name)
    logger.success(f"created {cont_name} container.")

    logger.info(f"waiting {waiting_iter} times each {waiting_sec} seconds for the {cont_name} to apply.")
    success = False
    for i in range(waiting_iter):
        time.sleep(waiting_sec)
        log = chi.container.get_logs(cont_name)
        if key_str:
            if key_str in log:
                success = True
                break

    if key_str:
        if success:
            logger.success(f"{cont_name} was successful.")
            if verbose:
                logger.success(log)
        else:
            logger.warning(f"{cont_name} was not successful.")
            if verbose:
                logger.warning(log)
    else:
        if verbose:
            logger.info(log)

    status = get_container_status(cont_name)
    if status:
        chi.container.destroy_container(cont_name)
        wait_until_container_removed(cont_name)
