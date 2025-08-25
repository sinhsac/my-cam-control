import sys
import time
import json
import os
from datetime import datetime

from CamHelper import get_cam_config, invalid_cam_config, get_url, test_rtsp_connection, capture_frame_robust
from DbHelper import DbHelper, TableNames, ColNames, ActionStatus, FieldNames, ActionType
from SysConfig import SysConfig
from common import logger, str2dict, FRAME_FOLDER

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
    """
    Execute camera actions based on command type
    Args:
        action: Action record from database containing command and details
        addition: Additional parameters parsed from JSON
    """
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

        if command == ActionType.CAPTURE_AND_STITCHING:
            # Create timestamped directory for this capture session
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            capture_folder = os.path.join(FRAME_FOLDER, f"capture_{timestamp}")
            os.makedirs(capture_folder, exist_ok=True)
            
            captured_files = []
            capture_info = []  # Store camera positions and frame paths

            # Capture frames from all cameras
            for cam_info in cam_infos:
                try:
                    ip_address = cam_info[ColNames.IP_ADDRESS]
                    user = cam_info[ColNames.USER]
                    password = cam_info[ColNames.PASSWORD]
                    position = cam_info.get(ColNames.POSITION, 0)  # Get camera position if available
                    
                    rtsp_url = get_url(ip_address, user, password)
                    if not test_rtsp_connection(rtsp_url):
                        logger.error(f"Failed to connect to camera {ip_address}")
                        continue
                    
                    frame_path = os.path.join(capture_folder, f"frame_{ip_address.replace('.', '_')}.jpg")
                    if capture_frame_robust(rtsp_url, 1920, 1080, frame_path):
                        captured_files.append(frame_path)
                        capture_info.append({
                            'path': frame_path,
                            'position': position,
                            'ip': ip_address
                        })
                        logger.info(f"Successfully captured frame from {ip_address} at position {position}")
                    else:
                        logger.error(f"Failed to capture frame from {ip_address}")
                        
                except Exception as e:
                    logger.error(f"Error capturing from camera {cam_info[ColNames.IP_ADDRESS]}: {str(e)}")
            
            # Check capture results
            if len(captured_files) == 0:
                logger.error("No frames were captured successfully")
                action_status = ActionStatus.FAILED
            else:
                logger.info(f"Successfully captured {len(captured_files)} frames")
                
                # Sort frames by camera position if available
                capture_info.sort(key=lambda x: x['position'])
                
                # Store capture metadata
                metadata_path = os.path.join(capture_folder, 'capture_info.json')
                with open(metadata_path, 'w') as f:
                    json.dump({
                        'timestamp': timestamp,
                        'total_cameras': len(cam_infos),
                        'successful_captures': len(captured_files),
                        'captures': capture_info
                    }, f, indent=2)
                
                logger.info(f"Capture session completed. Files stored in {capture_folder}")

        elif command == ActionType.CHECK_CONFIG:
            for cam_info in cam_infos:
                ip_address = cam_info[ColNames.IP_ADDRESS]
                user = cam_info[ColNames.USER]
                password = cam_info[ColNames.PASSWORD]
                
                rtsp_url = get_url(ip_address, user, password)
                if test_rtsp_connection(rtsp_url):
                    logger.info(f"Camera connection test successful for {ip_address}")
                else:
                    logger.error(f"Camera connection test failed for {ip_address}")
                    action_status = ActionStatus.FAILED
        else:
            logger.warning(f"Unknown command: {command}")
            action_status = ActionStatus.FAILED

    except Exception as e:
        logger.error(f"Error executing action: {str(e)}")
        action_status = ActionStatus.FAILED
        
    finally:
        # Update action status in database
        logger.info(f"Updating action {action[ColNames.ID]} status to {action_status}")
        db.update_by_id(
            table=TableNames.ACTION,
            id_value=action[ColNames.ID],
            data={
                ColNames.STATUS: action_status,
                ColNames.UPDATED_AT: datetime.now()
            }
        )

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
