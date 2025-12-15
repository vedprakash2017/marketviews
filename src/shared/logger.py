import json
import time
from datetime import datetime

class CentralLogger:
    def __init__(self, module_name: str, redis_client=None):
        self.module_name = module_name
        self.redis = redis_client
        self._channel = "channel:logs"
    
    def _ensure_redis(self):
        if self.redis is None:
            try:
                import redis
                self.redis = redis.Redis(host='localhost', port=6379, decode_responses=True)
            except Exception as e:
                print(f"[Logger] Redis connection failed: {e}")
                self.redis = None
    
    def _log(self, level: str, message: str, **kwargs):
        log_entry = {
            "timestamp": time.time(),
            "datetime": datetime.now().isoformat(),
            "level": level,
            "module": self.module_name,
            "message": message,
            "extra": kwargs
        }
        
        self._print_to_console(level, message, **kwargs)
        
        self._ensure_redis()
        if self.redis:
            try:
                json_log = json.dumps(log_entry)
                self.redis.publish(self._channel, json_log)
            except:
                pass
    
    def _print_to_console(self, level: str, message: str, **kwargs):
        timestamp = datetime.now().strftime('%H:%M:%S')
        extra_str = ", ".join([f"{k}={v}" for k, v in kwargs.items()])
        
        if extra_str:
            print(f"[{timestamp}] [{self.module_name}] {message} ({extra_str})")
        else:
            print(f"[{timestamp}] [{self.module_name}] {message}")
    
    def debug(self, message: str, **kwargs):
        self._log("DEBUG", message, **kwargs)
    
    def info(self, message: str, **kwargs):
        self._log("INFO", message, **kwargs)
    
    def warning(self, message: str, **kwargs):
        self._log("WARNING", message, **kwargs)
    
    def error(self, message: str, **kwargs):
        self._log("ERROR", message, **kwargs)
    
    def critical(self, message: str, **kwargs):
        self._log("CRITICAL", message, **kwargs)
    
    def metric(self, name: str, value: float, **kwargs):
        self._log("METRIC", f"{name}={value}", **kwargs)

# Global logger cache
_loggers = {}

def get_logger(module_name: str, redis_config: dict = None) -> CentralLogger:
    if module_name not in _loggers:
        _loggers[module_name] = CentralLogger(module_name)
    return _loggers[module_name]
