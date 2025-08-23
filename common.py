import glob
import io
import json
import logging
import os
import shutil
import sys

NOT_FOUND="not found"
TIME_CHECK_UPDATED_IN_SECOND = 3600 # 60 phut

HOME_DIRECTORY = os.path.expanduser('~')

WORKSPACE = os.path.join(HOME_DIRECTORY, ".mycamcontrol")



LOG_FOLDER = os.path.join(WORKSPACE, "logs")
VIDEO_FOLDER = os.path.join(WORKSPACE, "videos")
FRAME_FOLDER = os.path.join(WORKSPACE, "frames")
BACKUP_FOLDER = os.path.join(WORKSPACE, "backups")
CONFIG_FOLDER = os.path.join(WORKSPACE, "config")

os.makedirs(WORKSPACE, exist_ok=True)
os.makedirs(LOG_FOLDER, exist_ok=True)
os.makedirs(VIDEO_FOLDER, exist_ok=True)
os.makedirs(FRAME_FOLDER, exist_ok=True)
os.makedirs(BACKUP_FOLDER, exist_ok=True)
os.makedirs(CONFIG_FOLDER, exist_ok=True)

CONTROL_FILE = os.path.join(CONFIG_FOLDER, "control.json")
CAM_INFO_FILE = os.path.join(CONFIG_FOLDER, "cam_info.json")
CONFIG_FILE = os.path.join(CONFIG_FOLDER, "config.json")
SYS_CONFIG_FILE = os.path.join(CONFIG_FOLDER, "sys_config.json")
DB_FILE = os.path.join(CONFIG_FOLDER, "my_cam.db")
MAX_RETRIES = 3


sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FOLDER + '/app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def get_disk_space_mb():
    try:
        total, used, free = shutil.disk_usage(".")
        return free // (1024 * 1024)  # Convert to MB
    except Exception as e:
        logger.error(f"Error getting disk space: {e}")
        return 0


def write_status(status):
    """Write control status"""
    try:
        with open(CONTROL_FILE, 'w') as f:
            json.dump(status, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Error writing status: {e}")


def read_status():
    """Read control status"""
    try:
        with open(CONTROL_FILE, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error reading status: {e}")
        return {"status": "stop", "processing": False}

def read_cam_info():
    """Read cam info"""
    default_res = {'cameras': [], 'updated_at': None}
    if not os.path.exists(CAM_INFO_FILE):
        write_cam_info(default_res)
        return default_res

    try:
        with open(CAM_INFO_FILE, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error reading cam info: {e}")
        return default_res

def write_cam_info(cam_info):
    """Write control status"""
    try:
        with open(CAM_INFO_FILE, 'w') as f:
            json.dump(cam_info, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Error writing cam info: {e}")

def get_session_frames():
    """Get all frames for current session"""
    frame_pattern = os.path.join(FRAME_FOLDER, "frame_*.jpg")
    frame_files = sorted(glob.glob(frame_pattern))
    return frame_files


def renumber_frames_for_video():
    """Renumber frames to ensure continuous sequence for FFmpeg"""
    frame_files = get_session_frames()

    if not frame_files:
        logger.error("No frames found to renumber")
        return False

    try:
        # Create temporary directory for renumbered frames
        temp_dir = os.path.join(FRAME_FOLDER, "temp_renumber")

        os.makedirs(temp_dir, exist_ok=True)

        # Renumber frames starting from 1
        for i, frame_file in enumerate(frame_files, 1):
            new_name = f"frame_{i:06d}.jpg"
            new_path = os.path.join(temp_dir, new_name)
            shutil.copy2(frame_file, new_path)

        # Remove original frames and temp directory
        for frame_file in frame_files:
            if os.path.exists(frame_file):
                os.remove(frame_file)

        # Move renumbered frames back
        for temp_file in os.listdir(temp_dir):
            src = os.path.join(temp_dir, temp_file)
            dst = os.path.join(FRAME_FOLDER, temp_file)
            shutil.move(src, dst)

        os.rmdir(temp_dir)

        logger.info(f"Renumbered {len(frame_files)} frames for video creation")
        return True

    except Exception as e:
        logger.error(f"Error renumbering frames: {e}")
        return False

def str2dict(xstr):
    try:
        return json.loads(xstr)
    except Exception as e:
        logger.error(f"Error converting string to json: {e}")
        return {}