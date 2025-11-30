import uuid
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Optional, Dict, Any
import time


class MessageType(IntEnum):
    REQUEST = 1
    RESPONSE = 2
    HEARTBEAT = 3
    SCALE_UP = 4
    SHUTDOWN = 5


class RequestType(IntEnum):
    Z1 = 1
    Z2 = 2
    Z3 = 3


class ResponseStatus(IntEnum):
    SUCCESS = 0
    ERROR = 1
    TIMEOUT = 2
    NODE_DOWN = 3


@dataclass
class Request:
    request_id: str = field(default_factory=lambda: str(uuid.uuid4())) # UUID auto-generated
    request_type: RequestType = RequestType.Z1                         # Z1, Z2, or Z3
    client_id: str = ""                                                # K1 or K2
    data: Dict[str, Any] = field(default_factory=dict)                 # K address, timestamp
    created_at: float = field(default_factory=time.time)
    enqueued_at: Optional[float] = None
    
    def priority(self) -> int:
        return self.request_type.value


@dataclass  
class Response:
    request_id: str
    status: ResponseStatus                                 # SUCCESS, ERROR, TIMEOUT, NODE_DOWN
    processor_id: str                                      # P21, P22, or P23
    result: Dict[str, Any] = field(default_factory=dict)
    processing_time: float = 0.0
    created_at: float = field(default_factory=time.time)
