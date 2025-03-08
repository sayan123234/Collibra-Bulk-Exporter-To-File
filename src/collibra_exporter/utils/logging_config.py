"""
Logging Configuration Module

This module provides functionality for configuring logging.
"""

import os
import time
import logging
from logging.handlers import RotatingFileHandler

def setup_logging(log_dir='logs', max_days=30):
    """
    Configure logging with both file and console handlers, saving logs with timestamps.
    
    Args:
        log_dir: Directory to store log files
        max_days: Maximum age of log files in days before they are deleted
        
    Returns:
        logging.Logger: Configured logger instance
    """
    # Create logs directory if it doesn't exist
    os.makedirs(log_dir, exist_ok=True)
    
    # Create timestamp for log filename
    timestamp = time.strftime('%Y%m%d_%H%M%S')
    log_filename = f'collibra_exporter_{timestamp}.log'
    log_file = os.path.join(log_dir, log_filename)

    # Create formatters and handlers
    file_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(filename)s:%(lineno)d | %(funcName)s | %(message)s'
    )
    console_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s'
    )

    # File handler with rotation
    file_handler = RotatingFileHandler(
        log_file, 
        maxBytes=10*1024*1024,  # 10MB
        backupCount=10          # Keep 10 backup files
    )
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(logging.DEBUG)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(logging.INFO)

    # Root logger configuration
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    
    # Remove any existing handlers
    logger.handlers = []
    
    # Add our handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    # Log the start of a new session
    logger.info("="*60)
    logger.info(f"Starting new logging session at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Log file created at: {log_file}")
    logger.info("="*60)

    # Clean up old logs
    cleanup_old_logs(log_dir, max_days, logger)

    return logger

def cleanup_old_logs(log_dir, max_days, logger):
    """
    Remove log files older than max_days.
    
    Args:
        log_dir: Directory containing log files
        max_days: Maximum age of log files in days
        logger: Logger instance for logging cleanup operations
    """
    current_time = time.time()
    logger.info(f"Checking for logs older than {max_days} days")
    
    for filename in os.listdir(log_dir):
        if filename.endswith('.log'):
            filepath = os.path.join(log_dir, filename)
            file_time = os.path.getmtime(filepath)
            
            if (current_time - file_time) > (max_days * 24 * 60 * 60):
                try:
                    os.remove(filepath)
                    logger.info(f"Removed old log file: {filename}")
                except Exception as e:
                    logger.warning(f"Could not remove old log file {filename}: {str(e)}")
