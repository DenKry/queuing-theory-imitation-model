
import time
import random
import threading
from typing import Optional, Dict

from nodes.base_node import BaseNode, NodeStatus
from common.message import Request, Response, ResponseStatus
from network.tcp_client import TCPClient
from network.tcp_server import TCPServer
from core.metrics import MetricsCollector
from config import CONFIG


class ClientNode(BaseNode): 
    def __init__(self, node_id: str, host: str, port: int, queue_host: str, queue_port: int,
                 allowed_request_types: list, metrics: MetricsCollector):
        super().__init__(node_id, host, port)
        self.queue_host = queue_host
        self.queue_port = queue_port
        self.allowed_request_types = allowed_request_types
        self.metrics = metrics
        
        self._tcp_client = TCPClient(queue_host, queue_port)
        self._tcp_server: Optional[TCPServer] = None            
        
        self._pending: Dict[str, dict] = {}
        
        self.successful_requests = 0
        self.failed_requests = 0
        self.retried_requests = 0
        
        self._response_listener: Optional[threading.Thread] = None
    
    def start(self) -> None:
        self._running = True
        self.set_status(NodeStatus.RUNNING)
        
        self._tcp_server = TCPServer(self.host, self.port, self.handle_message)
        self._tcp_server.start()
        
        if not self._tcp_client.connect():
            self._logger.error("Failed to connect to queue")
            self._running = False
            self.set_status(NodeStatus.DOWN)
            return
        
        gen_thread = threading.Thread(target=self._generate_requests)
        gen_thread.daemon = True
        gen_thread.start()
        
        timeout_thread = threading.Thread(target=self._check_timeouts)
        timeout_thread.daemon = True
        timeout_thread.start()
        
        self._logger.info(f"Client started on {self.host}:{self.port}, sending {[rt.name for rt in self.allowed_request_types]}")
    
    def _generate_requests(self) -> None:
        while self._running:
            try:
                req_type = random.choice(self.allowed_request_types)              
                request = Request(
                    request_type=req_type,
                    client_id=self.node_id,
                    data={
                        "timestamp": time.time(),
                        "client_host": self.host,
                        "client_port": self.port
                    }
                )
                
                self._pending[request.request_id] = {
                    "expected": {"P21", "P22", "P23"},
                    "received": set(),
                    "sent_time": time.time(),
                    "retry_count": 0,
                    "request_obj": request
                }
                
                self._tcp_client.send_request_no_response(request)
                self.metrics.record_request_sent(self.node_id)
                
                self._logger.debug(f"Sent request {request.request_id[:8]} type={req_type.name}")
                
            except Exception as e:
                self._logger.error(f"Error generating request: {e}")
            
            time.sleep(1.0 / CONFIG.request_generation_rate)
    
    def _handle_response(self, response: Optional[Response]) -> None:
        if response is None:
            return
        
        req_id = response.request_id
        if req_id not in self._pending:
            return
        
        pending = self._pending[req_id]
        
        if response.status == ResponseStatus.SUCCESS:
            pending["received"].add(response.processor_id)
            
            if pending["received"] == pending["expected"]:
                latency = time.time() - pending["sent_time"]
                self.successful_requests += 1
                self.metrics.record_request_received(self.node_id, True, latency)
                del self._pending[req_id]
                self._logger.debug(f"Request {req_id[:8]} completed successfully")
        else:
            if pending["retry_count"] < CONFIG.max_retries:
                pending["retry_count"] += 1
                pending["received"] = set()
                pending["sent_time"] = time.time()
                
                self._tcp_client.send_request_no_response(pending["request_obj"])
                self.retried_requests += 1
                self.metrics.record_request_retry(self.node_id)
                self._logger.debug(f"Request {req_id[:8]} failed, retrying (attempt {pending["retry_count"]}/{CONFIG.max_retries})")
            else:
                latency = time.time() - pending["sent_time"]
                self.failed_requests += 1
                self.metrics.record_request_received(self.node_id, False, latency)
                del self._pending[req_id]
                self._logger.debug(f"Request {req_id[:8]} failed after {pending["retry_count"]} retries: {response.status}")
    
    def _check_timeouts(self) -> None:
        while self._running:
            current_time = time.time()
            timed_out = []
            
            for req_id, pending in list(self._pending.items()):
                elapsed = current_time - pending["sent_time"]
                if elapsed > CONFIG.client_request_timeout:
                    timed_out.append(req_id)
            
            for req_id in timed_out:
                if req_id in self._pending:
                    pending = self._pending[req_id]
                    
                    if pending["retry_count"] < CONFIG.max_retries:
                        pending["retry_count"] += 1
                        pending["received"] = set()
                        pending["sent_time"] = time.time()
                        
                        self._tcp_client.send_request_no_response(pending["request_obj"])
                        self.retried_requests += 1
                        self.metrics.record_request_retry(self.node_id)
                        self._logger.debug(f"Request {req_id[:8]} timed out, retrying (attempt {pending["retry_count"]}/{CONFIG.max_retries})")
                    else:
                        latency = current_time - pending["sent_time"]
                        self.failed_requests += 1
                        self.metrics.record_request_received(self.node_id, False, latency)
                        del self._pending[req_id]
                        self._logger.debug(f"Request {req_id[:8]} timed out after {pending["retry_count"]} retries")
            
            time.sleep(1.0)
    
    def stop(self) -> None:
        self._running = False
        self._tcp_client.disconnect()
        if self._tcp_server:
            self._tcp_server.stop()
        self.set_status(NodeStatus.STOPPED)
        self._logger.info(f"Client stopped. Success: {self.successful_requests}, Failed: {self.failed_requests}, Retried: {self.retried_requests}")
    
    def handle_message(self, message, sender) -> Optional[Response]:
        if isinstance(message, Response):
            self._handle_response(message)
        return None
    
    def get_stats(self) -> dict:
        return {
            "node_id": self.node_id,
            "successful": self.successful_requests,
            "failed": self.failed_requests,
            "retried": self.retried_requests,
            "pending": len(self._pending)
        }
