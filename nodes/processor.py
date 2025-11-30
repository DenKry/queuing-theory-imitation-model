
import time
import random
import threading
from typing import Optional, List

from nodes.base_node import BaseNode, NodeStatus
from common.message import Request, Response, ResponseStatus
from common.distributions import ServiceTimeGenerator
from network.tcp_server import TCPServer
from network.tcp_client import TCPClient
from config import CONFIG


class ProcessorNode(BaseNode):
    def __init__(self, node_id: str, host: str, port: int,
                 forward_to: Optional[List[tuple]] = None,
                 can_fail: bool = False,
                 failure_probability: float = 0.0):

        super().__init__(node_id, host, port)
        
        self._forward_configs = forward_to or []
        self._forward_clients: List[TCPClient] = []
        self._can_fail = can_fail
        self._failure_probability = failure_probability
        
        self._server: Optional[TCPServer] = None
        self._current_request: Optional[Request] = None
        self._idle_since: float = time.time()
        
        self.requests_processed = 0
        self.requests_failed = 0
    
    def start(self) -> None:
        self._running = True
        self.set_status(NodeStatus.IDLE)
        
        for host, port in self._forward_configs:
            client = TCPClient(host, port)
            if client.connect():
                self._forward_clients.append(client)
                self._logger.debug(f"Connected to forward destination {host}:{port}")
        
        self._server = TCPServer(self.host, self.port, self.handle_message)
        self._server.start()
        
        if self._can_fail:
            idle_thread = threading.Thread(target=self._check_idle_timeout)
            idle_thread.daemon = True
            idle_thread.start()
        
        self._logger.info(f"Processor started on {self.host}:{self.port}")
    
    def handle_message(self, message, _sender) -> Optional[Response]:
        if not isinstance(message, Request):
            return None
        
        if self.status == NodeStatus.DOWN:
            return Response(
                request_id=message.request_id,
                status=ResponseStatus.NODE_DOWN,
                processor_id=self.node_id
            )
        
        self._idle_since = time.time()
        self.set_status(NodeStatus.BUSY)
        self._current_request = message
        
        if self._can_fail and random.random() < self._failure_probability:
            self.set_status(NodeStatus.DOWN)
            self.requests_failed += 1
            self._logger.warning(f"Processor failed while processing {message.request_id[:8]}")
            return Response(
                request_id=message.request_id,
                status=ResponseStatus.NODE_DOWN,
                processor_id=self.node_id
            )
        
        service_time = ServiceTimeGenerator.generate()
        time.sleep(service_time)
        
        self.requests_processed += 1
        self.set_status(NodeStatus.IDLE)
        self._idle_since = time.time()
        self._current_request = None
        
        if self._forward_clients:
            for client in self._forward_clients:
                if client.is_connected:
                    client.send_request_no_response(message)
            self._logger.debug(f"Forwarded {message.request_id[:8]} to {len(self._forward_clients)} destinations")
            return None
        
        response = Response(
            request_id=message.request_id,
            status=ResponseStatus.SUCCESS,
            processor_id=self.node_id,
            result={"processed_by": self.node_id},
            processing_time=service_time
        )
        
        if "client_host" in message.data and "client_port" in message.data:
            client = TCPClient(message.data["client_host"], message.data["client_port"])
            if client.connect():
                client.send_request_no_response(response)
                client.disconnect()
                self._logger.debug(f"Sent response for {message.request_id[:8]} to client")
        
        return None
    
    def _check_idle_timeout(self) -> None:
        while self._running:
            if self.status == NodeStatus.IDLE:
                idle_time = time.time() - self._idle_since
                if idle_time > CONFIG.idle_timeout:
                    self.set_status(NodeStatus.DOWN)
                    self._logger.warning(f"Processor went down due to idle timeout ({idle_time:.1f}s)")
            time.sleep(1.0)
    
    def recover(self) -> None:
        if self.status == NodeStatus.DOWN:
            self.set_status(NodeStatus.IDLE)
            self._idle_since = time.time()
            self.requests_failed = 0
            self._logger.info("Processor recovered")
    
    def stop(self) -> None:
        self._running = False
        if self._server:
            self._server.stop()
        for client in self._forward_clients:
            client.disconnect()
        self.set_status(NodeStatus.STOPPED)
        self._logger.info(f"Processor stopped. Processed: {self.requests_processed}, Failed: {self.requests_failed}")
    
    def get_stats(self) -> dict:
        return {
            "node_id": self.node_id,
            "status": self.status.value,
            "processed": self.requests_processed,
            "failed": self.requests_failed
        }
