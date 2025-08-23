import json
import os
import shutil
import signal
import subprocess
import time
from datetime import datetime

from CamHelper import test_rtsp_connection, capture_frame_robust, get_frame_sharpness
from FrameHelper import cleanup_session_frames, cleanup_old_backups
from common import logger, BACKUP_FOLDER, VIDEO_FOLDER, FRAME_FOLDER, CONFIG_FILE, get_disk_space_mb, \
    read_status, write_status, get_session_frames, renumber_frames_for_video


class TimelapseWorker:
    def __init__(self):
        self.frame_height = 720
        self.frame_width = 1280
        self.disk_warning_threshold = 1024
        self.codec = 'h264'
        self.output_fps = 15
        self.quality = "720p"
        self.interval = 10
        self.rtsp_url = None
        self.recording = False
        self.current_session = None
        self.frame_count = 0
        self.session_start_time = None
        self.error_count = 0
        self.last_good_frame = None
        self.running = True
        self.processing_video = False  # Trạng thái xử lý video

        # Load configuration
        self.load_config()

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def load_config(self):
        """Load configuration from config file"""
        try:
            with open(CONFIG_FILE, 'r') as f:
                config = json.load(f)

            self.rtsp_url = config.get("rtsp_url", "")
            self.interval = config.get("interval", 10)
            self.quality = config.get("quality", "720p")
            self.output_fps = config.get("output_fps", 5)
            self.codec = config.get("codec", "h264")
            self.frame_width = config.get("frame_width", 1280)
            self.frame_height = config.get("frame_height", 720)
            self.disk_warning_threshold = config.get("disk_warning_threshold", 1024)  # MB

            # Quality settings based on selection
            quality_settings = {
                "480p": (640, 480),
                "720p": (1280, 720),
                "1080p": (1920, 1080)
            }

            if self.quality in quality_settings:
                self.frame_width, self.frame_height = quality_settings[self.quality]

        except Exception as e:
            logger.error(f"Error loading config: {e}")
            pass

    def check_disk_space(self):
        """Kiểm tra dung lượng ổ đĩa có đủ không"""
        free_space_mb = get_disk_space_mb()
        if free_space_mb < self.disk_warning_threshold:
            logger.warning(f"Low disk space: {free_space_mb} MB < {self.disk_warning_threshold} MB")
            return False
        return True

    def signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False
        if self.recording:
            self.finalize_video()

    def update_processing_status(self, processing=False):
        """Cập nhật trạng thái processing"""
        try:
            status = read_status()
            status["processing"] = processing
            write_status(status)
            self.processing_video = processing
            logger.info(f"Processing status updated: {processing}")
        except Exception as e:
            logger.error(f"Error updating processing status: {e}")

    def start_new_session(self):
        """Start a new recording session with clean frame numbering"""
        self.session_start_time = datetime.now()
        self.frame_count = 0

        # Clean up any existing frames from previous sessions
        cleanup_session_frames()

        logger.info(f"Starting new session: {self.current_session}")

    def capture_best_frame(self):
        """Capture multiple frames and select the best one"""
        # Kiểm tra dung lượng ổ đĩa trước khi capture
        if not self.check_disk_space():
            logger.error("Insufficient disk space, skipping frame capture")
            return False

        temp_frames = []
        best_frame = None
        best_score = -1

        try:
            # Capture 3 frames over a short period for quality selection
            for i in range(3):
                temp_path = os.path.join(FRAME_FOLDER, f"temp_{i:03d}.jpg")

                if capture_frame_robust(self.rtsp_url, self.frame_width, self.frame_height, temp_path):
                    sharpness = get_frame_sharpness(temp_path)
                    temp_frames.append((temp_path, sharpness))

                    if sharpness > best_score:
                        best_score = sharpness
                        best_frame = temp_path

                    # Small delay between captures
                    time.sleep(0.5)
                else:
                    logger.warning(f"Failed to capture temp frame {i}")

            # Save best frame with proper sequential naming
            if best_frame and best_score > 0:
                # Use consistent naming: frame_000001.jpg, frame_000002.jpg, etc.
                final_path = os.path.join(FRAME_FOLDER, f"frame_{self.frame_count + 1:06d}.jpg")
                shutil.copy2(best_frame, final_path)

                # Backup good frames (giới hạn số lượng backup)
                backup_path = os.path.join(BACKUP_FOLDER, f"backup_{self.frame_count + 1:06d}.jpg")
                shutil.copy2(best_frame, backup_path)

                self.last_good_frame = final_path
                self.frame_count += 1

                logger.info(f"Captured frame {self.frame_count} with sharpness {best_score:.2f}")

                # Cleanup temp frames
                for temp_path, _ in temp_frames:
                    try:
                        os.remove(temp_path)
                    except Exception as e:
                        logger.error(f"Error deleting temp frame {temp_path}: {e}")
                        pass

                # Periodically clean old backups to save disk space
                if self.frame_count % 50 == 0:
                    cleanup_old_backups()

                return True
            else:
                logger.error("No good frames captured")
                return False

        except Exception as e:
            logger.error(f"Error in capture_best_frame: {e}")
            return False

    def create_video(self, output_path):
        """Create final video from captured frames"""
        try:
            # Cập nhật trạng thái processing = True
            self.update_processing_status(True)

            # First, get and validate frames
            frame_files = get_session_frames()

            if len(frame_files) < 2:
                logger.error(f"Not enough frames to create video. Found: {len(frame_files)}")
                self.update_processing_status(False)
                return False

            logger.info(f"Creating video from {len(frame_files)} frames")

            # Renumber frames to ensure continuous sequence
            if not renumber_frames_for_video():
                logger.error("Failed to renumber frames")
                self.update_processing_status(False)
                return False

            # Frame pattern for FFmpeg (starts from 1)
            frame_pattern = os.path.join(FRAME_FOLDER, "frame_%06d.jpg")

            # Verify first frame exists
            first_frame = os.path.join(FRAME_FOLDER, "frame_000001.jpg")
            if not os.path.exists(first_frame):
                logger.error(f"First frame not found: {first_frame}")
                # List available frames for debugging
                available_frames = [f for f in os.listdir(FRAME_FOLDER) if f.startswith("frame_")]
                logger.error(f"Available frames: {available_frames[:10]}...")  # Show first 10
                self.update_processing_status(False)
                return False

            # Create temporary output path
            temp_output = output_path.replace(".mp4", "_tmp.mp4")

            # Video encoding command
            cmd = [
                "ffmpeg",
                "-y",  # Overwrite output
                "-framerate", str(self.output_fps),
                "-start_number", "1",  # Start from frame 1
                "-i", frame_pattern,
                "-c:v", "libx264" if self.codec == "h264" else "libx265",
                "-preset", "medium",
                "-crf", "23",
                "-pix_fmt", "yuv420p",
                "-movflags", "+faststart",
                temp_output,
                "-loglevel", "info"  # Changed to info for better debugging
            ]

            logger.info(f"Running FFmpeg command: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)  # Tăng timeout

            if result.returncode == 0:
                # Move temp file to final location
                shutil.move(temp_output, output_path)
                logger.info(f"Video created successfully: {output_path}")

                # Cập nhật trạng thái processing = False
                self.update_processing_status(False)
                return True
            else:
                logger.error(f"Video creation failed: {result.stderr}")
                # Clean up temp file if it exists
                if os.path.exists(temp_output):
                    os.remove(temp_output)

                # Cập nhật trạng thái processing = False
                self.update_processing_status(False)
                return False

        except Exception as e:
            logger.error(f"Error creating video: {e}")
            # Cập nhật trạng thái processing = False trong trường hợp lỗi
            self.update_processing_status(False)
            return False

    def finalize_video(self):
        """Finalize current recording session"""
        if self.current_session and self.frame_count > 0:
            logger.info(f"Finalizing video with {self.frame_count} frames...")
            output_path = os.path.join(VIDEO_FOLDER, self.current_session)

            if self.create_video(output_path):
                logger.info(f"Video finalized: {self.current_session}")
                # Clean up session frames after successful video creation
                cleanup_session_frames()
            else:
                logger.error("Failed to create final video - keeping frames for debugging")
                # Vẫn cần cập nhật processing status về False nếu thất bại
                self.update_processing_status(False)

            # Clean up old backups regardless
            cleanup_old_backups()

            self.current_session = None
            self.frame_count = 0
            self.session_start_time = None

    def run(self):
        """Main worker loop"""
        logger.info("🎬 Timelapse Worker Started - Improved Version with Disk Management")

        # Test RTSP connection at startup
        if not test_rtsp_connection(self.rtsp_url):
            logger.error("RTSP connection failed at startup!")
            return

        while self.running:
            try:
                # Reload config periodically
                self.load_config()

                status = read_status()

                if status["status"] == "start":
                    if not self.recording:
                        # Kiểm tra xem có đang processing video không
                        if status.get("processing", False):
                            logger.warning("Cannot start recording while processing video")
                            time.sleep(5)
                            continue

                        # Kiểm tra dung lượng ổ đĩa trước khi bắt đầu
                        if not self.check_disk_space():
                            logger.error("Insufficient disk space to start recording")
                            time.sleep(30)  # Wait and check again
                            continue

                        logger.info("🎥 Starting new timelapse recording")
                        self.recording = True
                        self.current_session = status.get("current_video", "output.mp4")
                        self.start_new_session()
                        self.error_count = 0

                    # Capture best frame
                    if self.capture_best_frame():
                        self.error_count = 0
                    else:
                        self.error_count += 1
                        logger.warning(f"Frame capture failed, error count: {self.error_count}")

                        # If too many errors, try to recover
                        if self.error_count >= 5:
                            logger.error("Too many capture errors, attempting recovery...")
                            time.sleep(30)  # Wait before retry
                            if not test_rtsp_connection(self.rtsp_url):
                                logger.error("RTSP connection lost, waiting for recovery...")
                                time.sleep(60)
                            self.error_count = 0

                    # Log disk space info periodically (every 20 frames)
                    if self.frame_count % 20 == 0:
                        free_space = get_disk_space_mb()
                        logger.info(f"Disk space remaining: {free_space} MB")

                    # Wait for next capture
                    time.sleep(self.interval)

                else:
                    if self.recording:
                        logger.info("⏹ Stopping timelapse recording")
                        self.finalize_video()
                        self.recording = False

                    # When not recording, check less frequently
                    time.sleep(5)

            except KeyboardInterrupt:
                logger.info("Keyboard interrupt received")
                break
            except Exception as e:
                logger.error(f"Unexpected error in main loop: {e}")
                time.sleep(10)

        # Cleanup on exit
        if self.recording:
            self.finalize_video()

        logger.info("🛑 Timelapse Worker Stopped")
