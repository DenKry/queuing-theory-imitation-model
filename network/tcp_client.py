
import socket
import threading
from typing import Optional, Callable

from common.protocol import Protocol
from common.message import Request, Response
from common.logger import get_logger

logger = get_logger(__name__)


class TCPClient: 
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self._socket: Optional[socket.socket] = None
        self._connected = False
        self._lock = threading.Lock()
        self._response_handler: Optional[Callable] = None
    
    def connect(self) -> bool:
        try:
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._socket.connect((self.host, self.port))
            self._connected = True
            logger.debug(f"Connected to {self.host}:{self.port}")
            return True
        except Exception as e:
            logger.error(f"Connection failed to {self.host}:{self.port}: {e}")
            return False

   # synchronously wait for server response 
    def send_request(self, request: Request) -> Optional[Response]:
        if not self._connected:
            return None
        
        try:
            with self._lock:
                data = Protocol.serialize_request(request)
                self._socket.send(data)

                self._socket.settimeout(5.0)
                response_data = Protocol.receive_message(self._socket)
                if response_data:
                    return Protocol.deserialize(response_data)
                return None
        except socket.timeout:
            logger.debug(f"Request timeout to {self.host}:{self.port}")
            return None
        except Exception as e:
            logger.error(f"Send error: {e}")
            return None
    
    # asynchronous
    def send_request_no_response(self, message) -> bool:
        if not self._connected:
            return False
        
        try:
            with self._lock:
                # Serialize
                from common.message import Request, Response
                if isinstance(message, Request):
                    data = Protocol.serialize_request(message)
                elif isinstance(message, Response):
                    data = Protocol.serialize_response(message)
                else:
                    return False
                self._socket.send(data)
                return True
        except Exception as e:
            logger.debug(f"Send error: {e}")
            return False
    
    def send_request_async(self, request: Request, callback: Callable) -> None:
        def _send():
            response = self.send_request(request)
            callback(response)
        
        thread = threading.Thread(target=_send)
        thread.daemon = True
        thread.start()
    
    def disconnect(self) -> None:
        self._connected = False
        if self._socket:
            try:
                self._socket.close()
            except:
                pass
        logger.debug(f"Disconnected from {self.host}:{self.port}")
    
    @property
    def is_connected(self) -> bool:
        return self._connected
