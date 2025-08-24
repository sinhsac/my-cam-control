import sys
import time
from datetime import datetime

from CamHelper import get_cam_config, \
    invalid_cam_config
from DbHelper import DbHelper, TableNames, ColumnNames, ActionStatus
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
                    ColumnNames.IP_ADDRESS: cam_item['ip'],
                    ColumnNames.IP_TYPE: cam_item['type'],
                    ColumnNames.MAC_ADDRESS: cam_item['mac'],
                    ColumnNames.UPDATED_AT: datetime.now()
                })
            db.insert_or_update_batch_precise(
                table=TableNames.CAMERA,
                data_list=data_list,
                unique_columns=[ColumnNames.MAC_ADDRESS],
                update_columns=[ColumnNames.IP_ADDRESS, ColumnNames.UPDATED_AT]
            )

        action = db.select_first_order_by(table=TableNames.ACTION,
                                          conditions=f"{ColumnNames.STATUS} = '{ActionStatus.PENDING}'",
                                          col_name=ColumnNames.CREATED_AT,
                                          sort_type='desc')
        if not action:
            time.sleep(2)
            return True

        db.update_by_id(table=TableNames.ACTION,
                        id_value=action[ColumnNames.ID],
                        data={ColumnNames.STATUS: ActionStatus.IN_PROGRESS})

        addition = str2dict(action[ColumnNames.ADDITIONS])

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
    try:
        command = action[ColumnNames.COMMAND]
        if command == 'capture_and_stitch' and ColumnNames.MAC_ADDRESS in addition:
            mac_address = addition[ColumnNames.MAC_ADDRESS]
            cam_info = db.select_all(table=TableNames.CAMERA,
                                     conditions=f"{ColumnNames.MAC_ADDRESS} = '{mac_address}'",
                                     limit=1)
            if cam_info:
                cam_info = cam_info[0]

            if cam_info:
                ip_address = cam_info[ColumnNames.IP_ADDRESS]
                logger.info(f"do command {command}, with cam IP {ip_address} here")
    finally:
        db.update_by_id(table=TableNames.ACTION,
                        id_value=action['id'],
                        data={ColumnNames.STATUS: ActionStatus.DONE})

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
