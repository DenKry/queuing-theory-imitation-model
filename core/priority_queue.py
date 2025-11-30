
from collections import deque
from typing import Optional, Dict, List
import time
import threading

from common.message import Request, RequestType

# priority-FIFO
class PriorityRequestQueue:
    def __init__(self):
        self._queues: Dict[RequestType, deque] = {
            RequestType.Z1: deque(),
            RequestType.Z2: deque(),
            RequestType.Z3: deque()
        }
        self._lock = threading.Lock()
        self._metrics: Dict[RequestType, List[float]] = {
            RequestType.Z1: [],
            RequestType.Z2: [],
            RequestType.Z3: []
        }
    
    def enqueue(self, request: Request) -> None:
        with self._lock:
            request.enqueued_at = time.time()
            self._queues[request.request_type].append(request)
    
    def dequeue(self) -> Optional[Request]:
        with self._lock:
            for req_type in [RequestType.Z3, RequestType.Z2, RequestType.Z1]:
                if self._queues[req_type]:
                    request = self._queues[req_type].popleft()
                    
                    # Record wait time
                    wait_time = time.time() - request.enqueued_at
                    self._metrics[req_type].append(wait_time)
                    return request
            return None
    
    def dequeue_by_type(self, req_type: RequestType) -> Optional[Request]:
        with self._lock:
            if self._queues[req_type]:
                request = self._queues[req_type].popleft()
                wait_time = time.time() - request.enqueued_at
                self._metrics[req_type].append(wait_time)
                return request
            return None
    
    # View highest priority request
    def peek(self) -> Optional[Request]:
        with self._lock:
            for req_type in [RequestType.Z3, RequestType.Z2, RequestType.Z1]:
                if self._queues[req_type]:
                    return self._queues[req_type][0]
            return None
    
    def size(self, req_type: Optional[RequestType] = None) -> int:
        with self._lock:
            if req_type:
                return len(self._queues[req_type])
            return sum(len(q) for q in self._queues.values())
    
    def get_average_wait_time(self, req_type: RequestType) -> float:
        with self._lock:
            times = self._metrics[req_type]
            if not times:
                return 0.0
            recent = times[-100:]   # last 100
            return sum(recent) / len(recent)
    
    def get_max_wait_time(self, req_type: RequestType) -> float:
        with self._lock:
            queue = self._queues[req_type]
            if not queue:
                return 0.0
            current_time = time.time()
            return max(current_time - req.enqueued_at for req in queue)
    
    def is_empty(self) -> bool:
        with self._lock:
            return all(len(q) == 0 for q in self._queues.values())
