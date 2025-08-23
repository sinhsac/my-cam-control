import os
import subprocess
import time

import numpy as np
from PIL import Image, ImageStat

from IPHelper import scan_ezviz_fast, get_arp_table, find_by_ip
from common import logger, MAX_RETRIES, read_cam_info, TIME_CHECK_UPDATED_IN_SECOND, write_cam_info

CHANNEL_1 = "ch1"
CHANNEL_2 = "ch2"

def test_rtsp_connection(rtsp_url):
    """Test RTSP connection before starting"""
    logger.info("Testing RTSP connection...")

    cmd = [
        "ffmpeg",
        "-rtsp_transport", "tcp",
        "-i", rtsp_url,
        "-t", "2",
        "-f", "null",
        "-",
        "-loglevel", "error"
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        if result.returncode == 0:
            logger.info("RTSP connection successful")
            return True
        else:
            logger.error(f"RTSP test failed: {result.stderr}")
            return False
    except subprocess.TimeoutExpired:
        logger.error("RTSP connection timeout")
        return False
    except Exception as e:
        logger.error(f"RTSP test error: {e}")
        return False


def capture_with_tcp_transport(rtsp_url, frame_width, frame_height, output_path):
    """Method 1: TCP transport with H.264 forcing"""
    cmd = [
        "ffmpeg",
        "-rtsp_transport", "tcp",
        "-allowed_media_types", "video",
        "-i", rtsp_url,
        "-vframes", "1",
        "-vf", f"scale={frame_width}:{frame_height}",
        "-c:v", "mjpeg",  # Force MJPEG output to avoid HEVC issues
        "-q:v", "3",  # High quality
        "-y",
        output_path,
        "-loglevel", "error"
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    return result.returncode == 0


def capture_with_udp_transport(rtsp_url, frame_width, frame_height, output_path):
    """Method 2: UDP transport"""
    cmd = [
        "ffmpeg",
        "-rtsp_transport", "udp",
        "-i", rtsp_url,
        "-vframes", "1",
        "-vf", f"scale={frame_width}:{frame_height}",
        "-c:v", "mjpeg",
        "-q:v", "3",
        "-y",
        output_path,
        "-loglevel", "error"
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    return result.returncode == 0


def capture_with_different_codec(rtsp_url, frame_width, frame_height, output_path):
    """Method 3: Different codec approach"""
    cmd = [
        "ffmpeg",
        "-rtsp_transport", "tcp",
        "-fflags", "+genpts",
        "-thread_queue_size", "512",
        "-i", rtsp_url,
        "-vframes", "1",
        "-vf", f"scale={frame_width}:{frame_height},format=yuv420p",
        "-f", "image2",
        "-y",
        output_path,
        "-loglevel", "warning"
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    return result.returncode == 0

def capture_frame_robust(rtsp_url, frame_width, frame_height, output_path, retry_count=0):
    """Capture single frame with error handling and retry logic"""
    if retry_count >= MAX_RETRIES:
        logger.error("Max retries reached for frame capture")
        return False

    # Sử dụng multiple methods để capture frame
    methods = [
        capture_with_tcp_transport,
        capture_with_udp_transport,
        capture_with_different_codec
    ]

    for i, method in enumerate(methods):
        try:
            if method(rtsp_url, frame_width, frame_height, output_path):
                # Validate captured frame
                if validate_frame(output_path):
                    return True
                else:
                    logger.warning(f"Frame validation failed with method {i + 1}")
                    continue
        except Exception as e:
            logger.error(f"Capture method {i + 1} failed: {e}")
            continue

    # If all methods failed, try again with delay
    if retry_count < MAX_RETRIES:
        logger.warning(f"All capture methods failed, retrying in 5s... ({retry_count + 1}/{MAX_RETRIES})")
        time.sleep(5)
        return capture_frame_robust(rtsp_url, frame_width, frame_height, output_path, retry_count + 1)

    return False


def get_frame_sharpness(frame_path):
    """Calculate frame sharpness using Laplacian variance"""
    try:
        with Image.open(frame_path) as img:
            # Convert to grayscale
            gray = img.convert('L')
            arr = np.array(gray, dtype=np.float32)

            # Calculate Laplacian
            laplacian = np.abs(np.gradient(arr, axis=0)) + np.abs(np.gradient(arr, axis=1))

            # Return variance (higher = sharper)
            return np.var(laplacian)
    except Exception as e:
        logger.error(f"Error calculating sharpness for {frame_path}: {e}")
        return 0


def validate_frame(frame_path):
    """Validate captured frame quality"""
    try:
        if not os.path.exists(frame_path):
            return False

        # Check file size (should be reasonable)
        file_size = os.path.getsize(frame_path)
        if file_size < 1000:  # Too small, likely corrupted
            logger.warning(f"Frame too small: {file_size} bytes")
            return False

        # Check if image can be opened
        with Image.open(frame_path) as img:
            # Check dimensions
            if img.size[0] < 100 or img.size[1] < 100:
                logger.warning(f"Frame dimensions too small: {img.size}")
                return False

            # Check if image is not completely black or white
            stat = ImageStat.Stat(img.convert('L'))
            brightness = stat.mean[0]
            if brightness < 5 or brightness > 250:
                logger.warning(f"Frame brightness unusual: {brightness}")
                return False

            return True

    except Exception as e:
        logger.error(f"Error validating frame {frame_path}: {e}")
        return False

def get_url(ip, user, password, channel=CHANNEL_1, encode='h264'):
    return f"rtsp://{user}:{password}@{ip}:554/{encode}/{channel}/main/av_stream"

def invalid_cam_config(cam_config):
    return cam_config is None or cam_config['updated_at'] is None or len(cam_config['cameras']) == 0

def get_cam_config():
    cam_info = read_cam_info()
    if not invalid_cam_config(cam_info) and (time.time() - cam_info['updated_at']) < TIME_CHECK_UPDATED_IN_SECOND:
        logger.debug('cam config is still valid')
        return cam_info, True
    cameras_ips = scan_ezviz_fast()
    if cameras_ips:
        ips = get_arp_table()
        cameras = []
        for cam_ip in cameras_ips:
            mac_info = find_by_ip(cam_ip, ips)
            if mac_info is None:
                continue
            cameras.append(mac_info)
        if len(cameras) == 0:
            return cam_info, False

        cam_info = {
            'selected': cameras[0],
            'cameras': cameras,
            'updated_at': time.time()
        }

        write_cam_info(cam_info)
        return cam_info, False
    logger.warning('cannot find any cameras')
    return cam_info, False