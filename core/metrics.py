
from dataclasses import dataclass, field
from typing import Dict, List
from collections import defaultdict
import time
import threading


@dataclass
class RequestMetrics:
    total_sent: int = 0
    total_received: int = 0
    successful: int = 0
    failed: int = 0
    retried: int = 0
    total_latency: float = 0.0
    latencies: List[float] = field(default_factory=list)


class MetricsCollector:
    def __init__(self):
        self._lock = threading.Lock()
        self._client_metrics: Dict[str, RequestMetrics] = defaultdict(RequestMetrics)
        self._queue_lengths: Dict[str, List[tuple]] = defaultdict(list)  # (timestamp, length)
        self._wait_times: Dict[str, List[float]] = defaultdict(list)
        self._processing_times: Dict[str, List[float]] = defaultdict(list)
        self._start_time = time.time()
    
    def record_request_sent(self, client_id: str) -> None:
        with self._lock:
            self._client_metrics[client_id].total_sent += 1
    
    def record_request_retry(self, client_id: str) -> None:
        with self._lock:
            self._client_metrics[client_id].retried += 1
    
    def record_request_received(self, client_id: str, success: bool, latency: float) -> None:
        with self._lock:
            metrics = self._client_metrics[client_id]
            metrics.total_received += 1
            if success:
                metrics.successful += 1
            else:
                metrics.failed += 1
            metrics.total_latency += latency
            metrics.latencies.append(latency)
    
    def record_queue_length(self, queue_id: str, length: int) -> None:
        with self._lock:
            self._queue_lengths[queue_id].append((time.time(), length))
    
    def record_wait_time(self, queue_id: str, wait_time: float) -> None:
        with self._lock:
            self._wait_times[queue_id].append(wait_time)
    
    def record_processing_time(self, processor_id: str, proc_time: float) -> None:
        with self._lock:
            self._processing_times[processor_id].append(proc_time)
    
    def get_summary(self, pending_by_client: Dict[str, int] = None) -> Dict:
        with self._lock:
            total_sent = sum(m.total_sent for m in self._client_metrics.values())
            total_successful = sum(m.successful for m in self._client_metrics.values())
            total_failed = sum(m.failed for m in self._client_metrics.values())
            total_retried = sum(m.retried for m in self._client_metrics.values())
            total_latency = sum(m.total_latency for m in self._client_metrics.values())
            total_received = sum(m.total_received for m in self._client_metrics.values())
            total_pending = sum(pending_by_client.values()) if pending_by_client else 0
            total_completed = total_successful + total_failed
            
            avg_latency = total_latency / total_received if total_received > 0 else 0.0
            
            success_rate_completed = total_successful / total_completed if total_completed > 0 else 0.0
            success_rate_overall = total_successful / total_sent if total_sent > 0 else 0.0
            completion_rate = total_completed / total_sent if total_sent > 0 else 0.0
            retry_rate = total_retried / total_sent if total_sent > 0 else 0.0
            
            duration = time.time() - self._start_time
            throughput = total_successful / duration if duration > 0 else 0.0
            
            return {
                "total_requests_sent": total_sent,
                "total_successful": total_successful,
                "total_failed_permanently": total_failed,
                "total_retried": total_retried,
                "total_pending": total_pending,
                "total_completed": total_completed,
                "success_rate_of_completed": success_rate_completed,
                "success_rate_overall": success_rate_overall,
                "completion_rate": completion_rate,
                "retry_rate": retry_rate,
                "average_latency": avg_latency,
                "simulation_duration": duration,
                "throughput": throughput
            }
    
    def get_queue_stats(self, queue_id: str) -> Dict:
        with self._lock:
            wait_times = self._wait_times.get(queue_id, [])
            avg_wait = sum(wait_times) / len(wait_times) if wait_times else 0.0
            max_wait = max(wait_times) if wait_times else 0.0
            return {
                "avg_wait_time": avg_wait,
                "max_wait_time": max_wait,
                "total_processed": len(wait_times)
            }
