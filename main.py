import sys
import time
from datetime import datetime

from CamHelper import get_cam_config, \
    invalid_cam_config, get_url, test_rtsp_connection
from DbHelper import DbHelper, TableNames, ColNames, ActionStatus, FieldNames, ActionType
from SysConfig import SysConfig
from common import logger, str2dict

sys_config = SysConfig()

running = True
db = DbHelper()
db.update_by_sys_config(sys_config)

def do_worker():
    try:
        cam_config, is_cached = get_cam_config()
        if invalid_cam_config(cam_config):
            logger.error("Invalid Camera Config")
            time.sleep(5)
            return False

        if not is_cached:
            logger.info('force update all cam')
            data_list = []
            for cam_item in cam_config['cameras']:
                data_list.append({
                    ColNames.IP_ADDRESS: cam_item['ip'],
                    ColNames.IP_TYPE: cam_item['type'],
                    ColNames.MAC_ADDRESS: cam_item['mac'],
                    ColNames.UPDATED_AT: datetime.now()
                })
            db.insert_or_update_batch_precise(
                table=TableNames.CAMERA,
                data_list=data_list,
                unique_columns=[ColNames.MAC_ADDRESS],
                update_columns=[ColNames.IP_ADDRESS, ColNames.UPDATED_AT]
            )

        action = db.select_first_order_by(table=TableNames.ACTION,
                                          conditions=f"{ColNames.STATUS} = '{ActionStatus.PENDING}'",
                                          col_name=ColNames.CREATED_AT,
                                          sort_type='desc')
        if not action:
            time.sleep(2)
            return True

        db.update_by_id(table=TableNames.ACTION,
                        id_value=action[ColNames.ID],
                        data={ColNames.STATUS: ActionStatus.IN_PROGRESS})

        addition = str2dict(action[ColNames.ADDITIONS])

        do_action(action, addition)

        time.sleep(3)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
        return False
    except Exception as ee:
        logger.error(f"Unexpected error in do worker: {ee}")
        time.sleep(10)
    return True


def do_action(action, addition):
    action_status = ActionStatus.DONE
    try:
        command = action[ColNames.COMMAND]
        if FieldNames.MAC_ADDRESSES in addition:
            mac_addresses = addition[FieldNames.MAC_ADDRESSES]
            joined_macs = "', '".join(mac_addresses)
            final_string = "'" + joined_macs + "'"
            cam_infos = db.select_all(table=TableNames.CAMERA,
                                     conditions=f"{ColNames.MAC_ADDRESS} IN ({final_string})")
        else:
            logger.warning('no camera choose')
            action_status = ActionStatus.FAILED
            return

        if not cam_infos:
            logger.warning(f"not found cameras for macs: {final_string}")
            action_status = ActionStatus.FAILED
            return

        logger.info(f"found {len(cam_infos)} cameras for macs: {final_string}")

        for cam_info in cam_infos:
            ip_address = cam_info[ColNames.IP_ADDRESS]
            user = cam_info[ColNames.USER]
            password = cam_info[ColNames.PASSWORD]
            logger.info(f"do command {command}, with cam IP {ip_address} here")
            if command == ActionType.CHECK_CONFIG:
                rtsp_url = get_url(ip_address, user, password)
                if test_rtsp_connection(rtsp_url):
                    action_status = ActionStatus.DONE
                    logger.info(f"this cam with url {rtsp_url} is working")


    finally:
        logger.info(f"finally update action {action[ColNames.ID]} to {action_status}")
        db.update_by_id(table=TableNames.ACTION,
                        id_value=action[ColNames.ID],
                        data={
                            ColNames.STATUS: action_status,
                            ColNames.UPDATED_AT: datetime.now()
                        })

try:
    while running:
        if not do_worker():
            break
except KeyboardInterrupt:
    logger.info("Worker stopped by user")
except Exception as e:
    logger.error(f"Worker crashed: {e}")
finally:
    db.close_connection()
    logger.info('App destroyed')
    sys.exit(0)
