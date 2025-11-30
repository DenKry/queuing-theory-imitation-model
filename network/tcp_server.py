
import socket
import threading
from typing import Callable, Optional, List

from common.protocol import Protocol
from common.logger import get_logger

logger = get_logger(__name__)


class TCPServer:       # Async TCP server
    
    def __init__(self, host: str, port: int, handler: Callable):
        self.host = host
        self.port = port
        self.handler = handler
        self._socket: Optional[socket.socket] = None
        self._running = False
        self._clients: List[socket.socket] = []
        self._lock = threading.Lock()
    
    def start(self) -> None:
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind((self.host, self.port))
        self._socket.listen(10)
        self._running = True
        
        logger.info(f"TCP Server started on {self.host}:{self.port}")
        
        accept_thread = threading.Thread(target=self._accept_connections)
        accept_thread.daemon = True
        accept_thread.start()
    
    def _accept_connections(self) -> None:
        while self._running:
            try:
                self._socket.settimeout(1.0)
                try:
                    client_socket, address = self._socket.accept()
                    logger.debug(f"New connection from {address}")
                    
                    with self._lock:
                        self._clients.append(client_socket)
                    
                    # Handle new client in separate thread (independently)
                    client_thread = threading.Thread(
                        target=self._handle_client,
                        args=(client_socket, address)
                    )
                    client_thread.daemon = True
                    client_thread.start()
                except socket.timeout:
                    continue
            except Exception as e:
                if self._running:
                    logger.error(f"Accept error: {e}")
    
    def _handle_client(self, client_socket: socket.socket, address) -> None:
        try:
            while self._running:
                try:
                    client_socket.settimeout(1.0)
                    data = Protocol.receive_message(client_socket)
                    if not data:
                        break
                    
                    message = Protocol.deserialize(data)
                    response = self.handler(message, address)
                    
                    if response:
                        from common.message import Response
                        if isinstance(response, Response):
                            client_socket.send(Protocol.serialize_response(response))
                except socket.timeout:
                    continue
                except Exception as e:
                    logger.debug(f"Client communication error: {e}")
                    break
        except Exception as e:
            logger.error(f"Client handler error: {e}")
        finally:
            with self._lock:
                if client_socket in self._clients:
                    self._clients.remove(client_socket)
            try:
                client_socket.close()
            except:
                pass
    
    def stop(self) -> None:
        self._running = False
        with self._lock:
            for client in self._clients:
                try:
                    client.close()
                except:
                    pass
            self._clients.clear()
        
        if self._socket:
            try:
                self._socket.close()
            except:
                pass
        logger.info(f"TCP Server stopped")
