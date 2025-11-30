
import time
import threading
from typing import Dict, Callable, Optional

from common.message import RequestType
from common.logger import get_logger
from config import CONFIG

logger = get_logger(__name__)


class ScalingMonitor:
    def __init__(self, 
                 get_queue_metrics: Callable,
                 scale_up_callback: Callable[[RequestType], None],
                 scale_down_callback: Callable[[RequestType], None]):

        self._get_metrics = get_queue_metrics
        self._scale_up = scale_up_callback
        self._scale_down = scale_down_callback
        self._running = False
        self._monitor_thread: Optional[threading.Thread] = None
        
        self._last_scale_time: Dict[RequestType, float] = {RequestType.Z1: 0, RequestType.Z2: 0, RequestType.Z3: 0}
        
        self._processor_counts: Dict[RequestType, int] = {
            RequestType.Z1: 1,  # P11
            RequestType.Z2: 1,  # P12
            RequestType.Z3: 1   # P13
        }
    
    def start(self) -> None:
        self._running = True
        self._monitor_thread = threading.Thread(target=self._monitor_loop)
        self._monitor_thread.daemon = True
        self._monitor_thread.start()
        logger.info("Scaling monitor started")
    
    def _monitor_loop(self) -> None:
        while self._running:
            try:
                metrics = self._get_metrics()
                current_time = time.time()
                
                for req_type in [RequestType.Z1, RequestType.Z2, RequestType.Z3]:
                    avg_wait_key = f"z{req_type.value}_avg_wait"
                    avg_wait = metrics.get(avg_wait_key, 0.0)
                    
                    time_since_last = current_time - self._last_scale_time[req_type]
                    
                    # Check for scale-up threshold
                    if avg_wait > CONFIG.avg_wait_time_threshold:
                        # Check cooldown (from last scale)
                        if time_since_last >= CONFIG.scaling_cooldown:
                            if self._processor_counts[req_type] < CONFIG.max_processors_per_type:
                                logger.info(f"Scaling up {req_type.name}: avg_wait={avg_wait:.2f}s > threshold={CONFIG.avg_wait_time_threshold}s")
                                self._scale_up(req_type)
                                self._processor_counts[req_type] += 1
                                self._last_scale_time[req_type] = current_time
                            else:
                                logger.warning(f"Can't scale {req_type.name}: max processors reached ({CONFIG.max_processors_per_type})")
                    
                    # Check for scale-down threshold
                    elif avg_wait < CONFIG.scale_down_threshold:
                        # Check cooldown (from last scale)
                        if time_since_last >= CONFIG.scaling_cooldown:
                            if self._processor_counts[req_type] > CONFIG.min_processors_per_type:
                                logger.info(f"Scaling down {req_type.name}: avg_wait={avg_wait:.2f}s < threshold={CONFIG.scale_down_threshold}s")
                                self._scale_down(req_type)
                                self._processor_counts[req_type] -= 1
                                self._last_scale_time[req_type] = current_time
            except Exception as e:
                logger.error(f"Error in scaling monitor: {e}")
            
            time.sleep(2.0)
    
    def stop(self) -> None:
        self._running = False
        logger.info("Scaling monitor stopped")
    
    def get_status(self) -> Dict:
        return {
            "processor_counts": {k.name: v for k, v in self._processor_counts.items()},
            "last_scale_times": {k.name: v for k, v in self._last_scale_time.items()}
        }
