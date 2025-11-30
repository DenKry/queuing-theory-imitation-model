
from typing import List, Optional, Dict
import threading


# j = (i mod m) + 1
class RoundRobinBalancer:   
    def __init__(self, server_ids: List[str]):
        self._servers = list(server_ids)
        self._server_status: Dict[str, bool] = {s: True for s in server_ids}
        self._last_index = -1
        self._lock = threading.Lock()
    
    @property
    def server_count(self) -> int:
        return len(self._servers)
    
    def get_next_server(self) -> Optional[str]:
        with self._lock:
            if not self._servers:
                return None
            
            available_servers = [s for s in self._servers if self._server_status[s]]
            if not available_servers:
                return None
            
            m = len(self._servers)
            for _ in range(m):
                # j converted to 0 indx
                next_index = (self._last_index + 1) % m
                self._last_index = next_index
                
                server_id = self._servers[next_index]
                if self._server_status[server_id]:
                    return server_id
            
            return None
    
    def mark_server_available(self, server_id: str) -> None:
        with self._lock:
            if server_id in self._server_status:
                self._server_status[server_id] = True
    
    def mark_server_unavailable(self, server_id: str) -> None:
        with self._lock:
            if server_id in self._server_status:
                self._server_status[server_id] = False
    
    # Dynamic scaling
    def add_server(self, server_id: str) -> None:
        with self._lock:
            if server_id not in self._servers:
                self._servers.append(server_id)
                self._server_status[server_id] = True
    
    def remove_server(self, server_id: str) -> None:
        with self._lock:
            if server_id in self._servers:
                self._servers.remove(server_id)
                del self._server_status[server_id]
    
    def get_available_count(self) -> int:
        with self._lock:
            return sum(1 for s in self._servers if self._server_status[s])
    
    def get_all_servers(self) -> List[str]:
        with self._lock:
            return list(self._servers)
