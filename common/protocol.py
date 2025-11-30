
import json
import struct
from typing import Union
from .message import Request, Response, MessageType, RequestType, ResponseStatus


# Messages to bytes for TCP transmission
class Protocol:
    HEADER_FORMAT = ">BH"  # B: message_type (1 byte); H: payload_length (2 bytes)
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    
    @staticmethod
    def serialize_request(request: Request) -> bytes:
        payload = json.dumps({
            "request_id": request.request_id,
            "request_type": request.request_type.value,
            "client_id": request.client_id,
            "data": request.data,
            "created_at": request.created_at,
            "enqueued_at": request.enqueued_at
        }).encode("utf-8")
        
        header = struct.pack(Protocol.HEADER_FORMAT, MessageType.REQUEST.value, len(payload))
        return header + payload
    
    @staticmethod
    def serialize_response(response: Response) -> bytes:
        payload = json.dumps({
            "request_id": response.request_id,
            "status": response.status.value,
            "processor_id": response.processor_id,
            "result": response.result,
            "processing_time": response.processing_time,
            "created_at": response.created_at
        }).encode("utf-8")
        
        header = struct.pack(Protocol.HEADER_FORMAT, MessageType.RESPONSE.value, len(payload))
        return header + payload
    
    @staticmethod
    def deserialize(data: bytes) -> Union[Request, Response]:
        msg_type, _ = struct.unpack(Protocol.HEADER_FORMAT, data[:Protocol.HEADER_SIZE])
        payload = json.loads(data[Protocol.HEADER_SIZE:].decode("utf-8"))
        
        if msg_type == MessageType.REQUEST.value:
            return Request(
                request_id=payload["request_id"],
                request_type=RequestType(payload["request_type"]),
                client_id=payload["client_id"],
                data=payload["data"],
                created_at=payload["created_at"],
                enqueued_at=payload.get("enqueued_at")
            )
        elif msg_type == MessageType.RESPONSE.value:
            return Response(
                request_id=payload["request_id"],
                status=ResponseStatus(payload["status"]),
                processor_id=payload["processor_id"],
                result=payload["result"],
                processing_time=payload["processing_time"],
                created_at=payload["created_at"]
            )
        else:
            raise ValueError(f"Unknown message type: {msg_type}")
    
    @staticmethod
    def receive_message(sock) -> bytes:
        header = sock.recv(Protocol.HEADER_SIZE)
        if not header:
            return None
        _, payload_len = struct.unpack(Protocol.HEADER_FORMAT, header)
        payload = sock.recv(payload_len)
        return header + payload
