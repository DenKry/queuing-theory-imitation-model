
from abc import ABC, abstractmethod
from enum import Enum
import threading
from typing import Optional

from common.logger import get_logger


class NodeStatus(Enum):
    INITIALIZING = "initializing"
    RUNNING = "running"
    BUSY = "busy"
    IDLE = "idle"
    DOWN = "down"
    STOPPED = "stopped"


class BaseNode(ABC): 
    def __init__(self, node_id: str, host: str = "localhost", port: int = 5000):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.status = NodeStatus.INITIALIZING
        self._running = False
        self._logger = get_logger(f"{self.__class__.__name__}_{node_id}")
        self._lock = threading.Lock()
    
    @abstractmethod
    def start(self) -> None:
        pass
    
    @abstractmethod
    def stop(self) -> None:
        pass
    
    @abstractmethod
    def handle_message(self, message, sender) -> Optional[any]:
        pass
    
    def set_status(self, status: NodeStatus) -> None:
        with self._lock:
            self.status = status
            self._logger.debug(f"Status changed to {status.value}")
    
    def is_running(self) -> bool:
        return self._running and self.status not in [NodeStatus.DOWN, NodeStatus.STOPPED]
