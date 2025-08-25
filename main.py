import cv2
import numpy as np
import sys
import time
import json
import os
from datetime import datetime
from typing import List, Dict

from CamHelper import get_cam_config, invalid_cam_config, get_url, test_rtsp_connection, capture_frame_robust
from DbHelper import DbHelper, TableNames, ColNames, ActionStatus, FieldNames, ActionType
from SysConfig import SysConfig
from common import logger, str2dict, FRAME_FOLDER

def stitch_images(images: List[str]) -> np.ndarray:
    """
    Stitch multiple images into a single panorama
    Args:
        images: List of image file paths to stitch
    Returns:
        Stitched image as numpy array
    """
    # Read all images
    imgs = [cv2.imread(img) for img in images]
    
    # Create stitcher object
    stitcher = cv2.Stitcher.create(cv2.Stitcher_PANORAMA)
    
    # Perform stitching
    status, stitched = stitcher.stitch(imgs)
    
    if status != cv2.Stitcher_OK:
        raise Exception(f"Stitching failed with status {status}")
        
    return stitched

def capture_channel(cam_info: Dict, channel: int, capture_folder: str) -> str:
    """
    Capture image from specific channel of a camera
    Args:
        cam_info: Camera information dictionary
        channel: Channel number to capture (1 or 2)
        capture_folder: Folder to save captured image
    Returns:
        Path to captured image file
    """
    ip_address = cam_info[ColNames.IP_ADDRESS]
    user = cam_info[ColNames.USER]
    password = cam_info[ColNames.PASSWORD]
    position = cam_info.get(ColNames.POSITION, 0)
    
    # Modify URL to include channel
    rtsp_url = get_url(ip_address, user, password, channel)
    
    if not test_rtsp_connection(rtsp_url):
        raise Exception(f"Failed to connect to camera {ip_address} channel {channel}")
    
    frame_path = os.path.join(capture_folder, f"frame_ch{channel}_{ip_address.replace('.', '_')}.jpg")
    if not capture_frame_robust(rtsp_url, 1920, 1080, frame_path):
        raise Exception(f"Failed to capture frame from {ip_address} channel {channel}")
        
    return frame_path

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
        if FieldNames.MAC_ADDRESSES not in addition:
            logger.warning('no camera choose')
            action_status = ActionStatus.FAILED
            return

        mac_addresses = addition[FieldNames.MAC_ADDRESSES]
        channels = addition.get('channels', [1])  # Default to channel 1 if not specified
        
        # Validate channels
        if not all(ch in [1, 2] for ch in channels):
            logger.error("Invalid channel specified. Only channels 1 and 2 are supported.")
            action_status = ActionStatus.FAILED
            return
            
        joined_macs = "', '".join(mac_addresses)
        final_string = f"'{joined_macs}'"
        cam_infos = db.select_all(
            table=TableNames.CAMERA,
            conditions=f"{ColNames.MAC_ADDRESS} IN ({final_string})"
        )

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
            capture_info = []

            # Capture frames from all cameras and channels
            for cam_info in cam_infos:
                for channel in channels:
                    try:
                        frame_path = capture_channel(cam_info, channel, capture_folder)
                        captured_files.append(frame_path)
                        capture_info.append({
                            'path': frame_path,
                            'position': cam_info.get(ColNames.POSITION, 0),
                            'ip': cam_info[ColNames.IP_ADDRESS],
                            'channel': channel
                        })
                        logger.info(f"Successfully captured frame from {cam_info[ColNames.IP_ADDRESS]} channel {channel}")
                    except Exception as e:
                        logger.error(f"Error capturing from camera {cam_info[ColNames.IP_ADDRESS]} channel {channel}: {str(e)}")
            
            # Check capture results
            if len(captured_files) == 0:
                logger.error("No frames were captured successfully")
                action_status = ActionStatus.FAILED
            else:
                logger.info(f"Successfully captured {len(captured_files)} frames")
                
                try:
                    # Group captures by channel
                    channel_captures = {}
                    for ch in channels:
                        ch_files = [info['path'] for info in capture_info if info['channel'] == ch]
                        if ch_files:
                            channel_captures[ch] = ch_files
                    
                    # Stitch images for each channel
                    stitched_images = {}
                    for ch, files in channel_captures.items():
                        try:
                            stitched = stitch_images(files)
                            stitched_path = os.path.join(capture_folder, f"stitched_ch{ch}.jpg")
                            cv2.imwrite(stitched_path, stitched)
                            stitched_images[ch] = stitched_path
                            logger.info(f"Successfully stitched images for channel {ch}")
                        except Exception as e:
                            logger.error(f"Failed to stitch images for channel {ch}: {str(e)}")
                            action_status = ActionStatus.FAILED
                    
                    # Store metadata
                    metadata_path = os.path.join(capture_folder, 'capture_info.json')
                    with open(metadata_path, 'w') as f:
                        json.dump({
                            'timestamp': timestamp,
                            'total_cameras': len(cam_infos),
                            'channels': channels,
                            'successful_captures': len(captured_files),
                            'captures': capture_info,
                            'stitched_images': stitched_images
                        }, f, indent=2)
                    
                    logger.info(f"Capture and stitching session completed. Files stored in {capture_folder}")
                    
                except Exception as e:
                    logger.error(f"Error in stitching process: {str(e)}")
                    action_status = ActionStatus.FAILED

        elif command == ActionType.CHECK_CONFIG:
            # Check configuration for all channels
            for cam_info in cam_infos:
                ip_address = cam_info[ColNames.IP_ADDRESS]
                user = cam_info[ColNames.USER]
                password = cam_info[ColNames.PASSWORD]
                
                for channel in channels:
                    rtsp_url = get_url(ip_address, user, password, channel)
                    if test_rtsp_connection(rtsp_url):
                        logger.info(f"Camera connection test successful for {ip_address} channel {channel}")
                    else:
                        logger.error(f"Camera connection test failed for {ip_address} channel {channel}")
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
