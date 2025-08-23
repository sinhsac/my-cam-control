import sys
import time
from datetime import datetime

from CamHelper import get_cam_config, \
    invalid_cam_config
from DbHelper import DbHelper
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
            logger.info('force delete cam')
            data_list = []
            for cam_item in cam_config['cameras']:
                data_list.append({
                    'ip_address': cam_item['ip'],
                    'ip_type': cam_item['type'],
                    'mac_address': cam_item['mac'],
                    'updated_at': datetime.now()
                })
            db.insert_or_update_batch_precise(
                table="xcam_cameras",
                data_list=data_list,
                unique_columns=["mac_address"],  # Check for duplicate by email
                update_columns=["ip_address", "updated_at"]  # Only update these columns
            )

        action = db.select_first_order_by(table="xcam_actions",
                                          conditions="status = 'pending'",
                                          col_name='created_at',
                                          sort_type='desc')
        if not action:
            time.sleep(2)
            return True

        db.update_by_id(table="xcam_actions",
                        id_value=action['id'],
                        data={'status': 'in_progress'})

        addition = str2dict(action['additions'])
        if action['command'] == 'capture_and_stitch' and 'mac_address' in addition:
            mac_address = addition['mac_address']
            cam_info = db.select_all(table='xcam_cameras',
                                     conditions=f"mac_address = '{mac_address}'",
                                     limit=1)
            if cam_info:
                cam_info = cam_info[0]

            if cam_info:
                ip_address = cam_info['ip_address']
                logger.info(f"do command {action['command']}, with cam IP {ip_address} here")

                db.update_by_id(table="xcam_actions",
                                id_value=action['id'],
                                data={'status': 'done'})
        time.sleep(3)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
        return False
    except Exception as ee:
        logger.error(f"Unexpected error in do worker: {ee}")
        time.sleep(10)
    return True

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
