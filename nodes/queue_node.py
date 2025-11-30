
import time
import threading
from typing import Dict, List, Optional

from nodes.base_node import BaseNode, NodeStatus
from common.message import Request, Response, RequestType
from network.tcp_server import TCPServer
from network.tcp_client import TCPClient
from core.priority_queue import PriorityRequestQueue
from core.round_robin import RoundRobinBalancer


class QueueNode(BaseNode):   
    def __init__(self, node_id: str, host: str, port: int,
                 processor_configs: Dict[RequestType, List[tuple]],
                 use_priority: bool = True,
                 use_round_robin: bool = True):
        super().__init__(node_id, host, port)
        
        self._queue = PriorityRequestQueue()
        self._processor_configs = processor_configs
        self._use_priority = use_priority
        self._use_round_robin = use_round_robin
        
        self._balancers: Dict[RequestType, RoundRobinBalancer] = {}
        self._processor_clients: Dict[str, TCPClient] = {}
        
        self._server: Optional[TCPServer] = None
        self._dispatch_thread: Optional[threading.Thread] = None
    
    def start(self) -> None:
        self._running = True
        self.set_status(NodeStatus.RUNNING)
        
        self._init_processor_connections()
        
        self._server = TCPServer(self.host, self.port, self.handle_message)
        self._server.start()
        
        self._dispatch_thread = threading.Thread(target=self._dispatch_loop)
        self._dispatch_thread.daemon = True
        self._dispatch_thread.start()
        
        self._logger.info(f"Queue node started on {self.host}:{self.port}")
    
    def _init_processor_connections(self) -> None:
        for req_type, configs in self._processor_configs.items():
            processor_ids = []
            for idx, (host, port) in enumerate(configs):
                proc_id = f"{req_type.name}_{idx}"
                processor_ids.append(proc_id)
                
                client = TCPClient(host, port)
                if client.connect():
                    self._processor_clients[proc_id] = client
                    self._logger.debug(f"Connected to processor {proc_id} at {host}:{port}")
                else:
                    self._logger.warning(f"Failed to connect to processor {proc_id}")
            
            if self._use_round_robin and processor_ids:
                self._balancers[req_type] = RoundRobinBalancer(processor_ids)
    
    def handle_message(self, message, _sender) -> Optional[Response]:
        if isinstance(message, Request):
            self._queue.enqueue(message)
            self._logger.debug(f"Enqueued request {message.request_id[:8]} type={message.request_type.name}")
            return None
        return None
    
    def _dispatch_loop(self) -> None:
        while self._running:
            if self._queue.is_empty():
                time.sleep(0.01)
                continue
            
            request = self._queue.dequeue()
            if request is None:
                continue
            
            proc_id = self._get_processor(request.request_type)
            if proc_id is None:
                self._queue.enqueue(request)
                time.sleep(0.1)
                continue
            
            client = self._processor_clients.get(proc_id)
            if client and client.is_connected:
                client.send_request_no_response(request)
                self._logger.debug(f"Dispatched {request.request_id[:8]} to {proc_id}")
            else:
                self._queue.enqueue(request)
                if proc_id in self._balancers.get(request.request_type, RoundRobinBalancer([])).get_all_servers():
                    self._balancers[request.request_type].mark_server_unavailable(proc_id)
    
    def _get_processor(self, req_type: RequestType) -> Optional[str]:
        if self._use_round_robin and req_type in self._balancers:
            return self._balancers[req_type].get_next_server()
        else:
            for proc_id, client in self._processor_clients.items():
                if req_type.name in proc_id and client.is_connected:
                    return proc_id
            return None
    
    def add_processor(self, req_type: RequestType, host: str, port: int) -> str:
        idx = len([p for p in self._processor_clients if req_type.name in p])
        proc_id = f"{req_type.name}_{idx}"
        
        client = TCPClient(host, port)
        if client.connect():
            self._processor_clients[proc_id] = client
            
            if req_type in self._balancers:
                self._balancers[req_type].add_server(proc_id)
            else:
                self._balancers[req_type] = RoundRobinBalancer([proc_id])
            
            self._logger.info(f"Added processor {proc_id}")
            return proc_id
        return None
    
    def remove_processor(self, req_type: RequestType, port: int) -> None:
        proc_to_remove = None
        for proc_id, client in list(self._processor_clients.items()):
            if req_type.name in proc_id and client.port == port:
                proc_to_remove = proc_id
                break
        
        if proc_to_remove:
            self._processor_clients[proc_to_remove].disconnect()
            del self._processor_clients[proc_to_remove]
            
            if req_type in self._balancers:
                self._balancers[req_type].remove_server(proc_to_remove)
            
            self._logger.info(f"Removed processor {proc_to_remove}")
    
    def get_queue_metrics(self) -> Dict:
        return {
            "total_size": self._queue.size(),
            "z1_size": self._queue.size(RequestType.Z1),
            "z2_size": self._queue.size(RequestType.Z2),
            "z3_size": self._queue.size(RequestType.Z3),
            "z1_avg_wait": self._queue.get_average_wait_time(RequestType.Z1),
            "z2_avg_wait": self._queue.get_average_wait_time(RequestType.Z2),
            "z3_avg_wait": self._queue.get_average_wait_time(RequestType.Z3),
        }
    
    def stop(self) -> None:
        self._running = False
        if self._server:
            self._server.stop()
        for client in self._processor_clients.values():
            client.disconnect()
        self.set_status(NodeStatus.STOPPED)
        self._logger.info("Queue node stopped")
