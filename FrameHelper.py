import os

from common import get_session_frames, logger, BACKUP_FOLDER


def cleanup_session_frames():
    """Clean up frames from current session only"""
    try:
        frame_files = get_session_frames()
        for frame_file in frame_files:
            os.remove(frame_file)

        logger.info(f"Cleaned up {len(frame_files)} session frames")

    except Exception as e:
        logger.error(f"Error cleaning up session frames: {e}")


def cleanup_old_backups():
    """Keep only recent backup frames to save disk space"""
    try:
        backup_files = sorted([f for f in os.listdir(BACKUP_FOLDER) if f.startswith("backup_")])
        if len(backup_files) > 100:  # Keep last 100 backups (increased from 50)
            files_to_remove = backup_files[:-100]
            for f in files_to_remove:
                os.remove(os.path.join(BACKUP_FOLDER, f))
            logger.info(f"Cleaned up {len(files_to_remove)} old backup frames")

    except Exception as e:
        logger.error(f"Error cleaning up old backups: {e}")
