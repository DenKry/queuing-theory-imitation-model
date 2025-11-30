
from typing import List, Optional

from nodes.base_node import BaseNode, NodeStatus
from common.message import Request
from network.tcp_server import TCPServer
from network.tcp_client import TCPClient


class DistributorNode(BaseNode):
    def __init__(self, node_id: str, host: str, port: int,
                 queue_configs: List[tuple]):
        super().__init__(node_id, host, port)
        
        self._queue_configs = queue_configs
        self._queue_clients: List[TCPClient] = []
        self._server: Optional[TCPServer] = None
        
        self.requests_distributed = 0
    
    def start(self) -> None:
        self._running = True
        self.set_status(NodeStatus.RUNNING)
        
        for host, port in self._queue_configs:
            client = TCPClient(host, port)
            if client.connect():
                self._queue_clients.append(client)
                self._logger.debug(f"Connected to queue at {host}:{port}")
            else:
                self._logger.warning(f"Failed to connect to queue at {host}:{port}")
        
        self._server = TCPServer(self.host, self.port, self.handle_message)
        self._server.start()
        
        self._logger.info(f"Distributor started, connected to {len(self._queue_clients)} queues")
    
    def handle_message(self, message, _sender) -> None:
        if not isinstance(message, Request):
            return None
        
        distributed_count = 0
        for client in self._queue_clients:
            if client.is_connected:
                if client.send_request_no_response(message):
                    distributed_count += 1
        
        if distributed_count > 0:
            self.requests_distributed += 1
            self._logger.debug(f"Distributed {message.request_id[:8]} to {distributed_count} queues")
        
        return None
    
    def stop(self) -> None:
        self._running = False
        if self._server:
            self._server.stop()
        for client in self._queue_clients:
            client.disconnect()
        self.set_status(NodeStatus.STOPPED)
        self._logger.info(f"Distributor stopped. Distributed: {self.requests_distributed}")
    
    def get_stats(self) -> dict:
        return {
            "node_id": self.node_id,
            "distributed": self.requests_distributed,
            "connected_queues": len([c for c in self._queue_clients if c.is_connected])
        }
