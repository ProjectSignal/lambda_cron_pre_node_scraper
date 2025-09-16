import logging
import sys
import time
import functools
from typing import Callable, Any, Optional
from botocore.exceptions import ClientError


def setup_logging():
    """Simple CloudWatch-compatible logging setup for Lambda"""
    # Configure root logger for CloudWatch
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        stream=sys.stdout
    )
    
    # Return logger instance
    return logging.getLogger('lambda_pre_node_scraper')


def get_logger(name):
    """Get a logger instance that inherits root configuration."""
    return logging.getLogger(name)


def retry_with_backoff(max_retries: int = 3, initial_delay: float = 1.0, 
                      backoff_factor: float = 2.0, exceptions: tuple = (Exception,)):
    """
    Decorator for retry logic with exponential backoff
    
    Args:
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay in seconds
        backoff_factor: Multiplier for delay after each retry
        exceptions: Tuple of exceptions to catch and retry on
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            delay = initial_delay
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    
                    if attempt == max_retries:
                        # Last attempt failed, raise the exception
                        raise e
                    
                    # Log the retry attempt
                    logger = get_logger(func.__module__)
                    logger.warning(f"Attempt {attempt + 1} failed for {func.__name__}: {e}. Retrying in {delay}s...")
                    
                    time.sleep(delay)
                    delay *= backoff_factor
            
            # This should never be reached, but just in case
            raise last_exception
        
        return wrapper
    return decorator


def validate_object_id(object_id_str: str) -> bool:
    """Validate that a string is a valid MongoDB ObjectId format"""
    try:
        from bson import ObjectId
        ObjectId(object_id_str)
        return True
    except Exception:
        return False


def safe_get_nested(data: dict, keys: list, default: Any = None) -> Any:
    """
    Safely get a nested value from a dictionary
    
    Args:
        data: The dictionary to search in
        keys: List of keys to traverse (e.g., ['geo', 'full'])
        default: Default value if key path doesn't exist
    
    Returns:
        The value at the nested key path or default value
    """
    current = data
    try:
        for key in keys:
            current = current[key]
        return current
    except (KeyError, TypeError):
        return default


def sanitize_string(value: str, max_length: int = None) -> Optional[str]:
    """
    Sanitize a string value for database storage
    
    Args:
        value: String to sanitize
        max_length: Optional maximum length to truncate to
    
    Returns:
        Sanitized string or None if invalid
    """
    if not isinstance(value, str):
        return None
    
    # Remove null bytes and other control characters
    sanitized = value.replace('\x00', '').strip()
    
    if not sanitized:
        return None
    
    if max_length and len(sanitized) > max_length:
        sanitized = sanitized[:max_length].rstrip()
    
    return sanitized


def format_processing_stats(stats: dict) -> str:
    """Format processing statistics for logging"""
    return (
        f"Processing stats: "
        f"processed={stats.get('processed', 0)}, "
        f"successful={stats.get('successful', 0)}, "
        f"failed={stats.get('failed', 0)}, "
        f"profiles_scraped={stats.get('profiles_scraped', 0)}"
    )


def calculate_duration(start_time: float) -> str:
    """Calculate and format duration from start time"""
    duration = time.time() - start_time
    
    if duration < 60:
        return f"{duration:.2f}s"
    elif duration < 3600:
        minutes = int(duration // 60)
        seconds = int(duration % 60)
        return f"{minutes}m {seconds}s"
    else:
        hours = int(duration // 3600)
        minutes = int((duration % 3600) // 60)
        return f"{hours}h {minutes}m"


def chunk_list(items: list, chunk_size: int) -> list:
    """Split a list into chunks of specified size"""
    chunks = []
    for i in range(0, len(items), chunk_size):
        chunks.append(items[i:i + chunk_size])
    return chunks


class Timer:
    """Simple context manager for timing operations"""
    
    def __init__(self, operation_name: str = "operation", logger: logging.Logger = None):
        self.operation_name = operation_name
        self.logger = logger or get_logger(__name__)
        self.start_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        self.logger.debug(f"Starting {self.operation_name}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.start_time:
            duration = calculate_duration(self.start_time)
            if exc_type:
                self.logger.error(f"{self.operation_name} failed after {duration}")
            else:
                self.logger.info(f"{self.operation_name} completed in {duration}")


def handle_lambda_timeout(timeout_buffer: int = 10):
    """
    Decorator to handle Lambda timeout gracefully
    
    Args:
        timeout_buffer: Seconds to reserve for cleanup before timeout
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Get context from args if available (Lambda context is usually second arg)
            context = None
            if len(args) > 1 and hasattr(args[1], 'get_remaining_time_in_millis'):
                context = args[1]
            
            if context:
                # Calculate available time
                remaining_time_ms = context.get_remaining_time_in_millis()
                available_time = (remaining_time_ms / 1000) - timeout_buffer
                
                logger = get_logger(func.__module__)
                logger.info(f"Available processing time: {available_time:.1f}s")
                
                # Store start time and available time in kwargs for function to use
                kwargs['_lambda_start_time'] = time.time()
                kwargs['_lambda_available_time'] = available_time
            
            return func(*args, **kwargs)
        
        return wrapper
    return decorator


def is_lambda_timeout_approaching(start_time: float, available_time: float, buffer: float = 5.0) -> bool:
    """
    Check if Lambda timeout is approaching
    
    Args:
        start_time: Processing start time
        available_time: Total available processing time
        buffer: Safety buffer in seconds
    
    Returns:
        True if timeout is approaching
    """
    elapsed = time.time() - start_time
    return elapsed >= (available_time - buffer)


def log_memory_usage():
    """Log current memory usage (Linux only, useful for Lambda)"""
    try:
        with open('/proc/self/status', 'r') as f:
            for line in f:
                if 'VmRSS' in line:
                    memory_kb = int(line.split()[1])
                    memory_mb = memory_kb / 1024
                    logger = get_logger(__name__)
                    logger.info(f"Current memory usage: {memory_mb:.1f} MB")
                    return memory_mb
    except Exception:
        # Not on Linux or can't read proc, ignore
        pass
    return None