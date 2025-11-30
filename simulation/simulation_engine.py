
import time
import random
from typing import Dict, List, Optional

from common.message import RequestType
from common.logger import get_logger
from core.metrics import MetricsCollector
from core.scaling_monitor import ScalingMonitor
from nodes.client import ClientNode
from nodes.queue_node import QueueNode
from nodes.processor import ProcessorNode
from nodes.distributor import DistributorNode
from config import CONFIG

logger = get_logger(__name__)


class SimulationEngine:
    def __init__(self):
        self.metrics = MetricsCollector()
        
        if CONFIG.random_seed is not None:
            random.seed(CONFIG.random_seed)
                    
        self.clients: List[ClientNode] = []
        self.queues: Dict[str, QueueNode] = {}
        self.processors: Dict[str, ProcessorNode] = {}
        self.distributor: Optional[DistributorNode] = None
        
        self.scaling_monitor: Optional[ScalingMonitor] = None
        
        self._next_port = CONFIG.tcp_port_base
    
    def _allocate_port(self) -> int:
        port = self._next_port
        self._next_port += 1
        return port
    
    def setup(self) -> None:
        logger.info("Setting up simulation...")
        
        # P2x procs
        p2x_ports = {}
        for i in range(1, 4):
            proc_id = f"P2{i}"
            port = self._allocate_port()
            p2x_ports[proc_id] = port
            
            processor = ProcessorNode(
                node_id=proc_id,
                host="localhost",
                port=port,
                forward_to=None,
                can_fail=True,
                failure_probability=CONFIG.p2x_failure_probability
            )
            self.processors[proc_id] = processor
        
        # Q2x queues (connect to P2x)
        q2x_ports = {}
        for i in range(1, 4):
            queue_id = f"Q2{i}"
            port = self._allocate_port()
            q2x_ports[queue_id] = port
            
            processor_configs = {
                RequestType.Z1: [("localhost", p2x_ports[f"P2{i}"])],
                RequestType.Z2: [("localhost", p2x_ports[f"P2{i}"])],
                RequestType.Z3: [("localhost", p2x_ports[f"P2{i}"])]
            }
            
            queue = QueueNode(
                node_id=queue_id,
                host="localhost",
                port=port,
                processor_configs=processor_configs,
                use_priority=False,
                use_round_robin=False
            )
            self.queues[queue_id] = queue
        
        # Distributor (connects to all Q2x)
        d_port = self._allocate_port()
        self.distributor = DistributorNode(
            node_id="D",
            host="localhost",
            port=d_port,
            queue_configs=[
                ("localhost", q2x_ports["Q21"]),
                ("localhost", q2x_ports["Q22"]),
                ("localhost", q2x_ports["Q23"])
            ]
        )
        
        # P1x processors (connect to D)
        p1x_ports = {}
        for i, _ in enumerate([RequestType.Z1, RequestType.Z2, RequestType.Z3], 1):
            proc_id = f"P1{i}"
            port = self._allocate_port()
            p1x_ports[proc_id] = port
            
            processor = ProcessorNode(
                node_id=proc_id,
                host="localhost",
                port=port,
                forward_to=[("localhost", d_port)],
                can_fail=False,
                failure_probability=0.0
            )
            self.processors[proc_id] = processor
        
        # Q1 (connects to P1x with round-robin)
        q1_port = self._allocate_port()
        q1 = QueueNode(
            node_id="Q1",
            host="localhost",
            port=q1_port,
            processor_configs={
                RequestType.Z1: [("localhost", p1x_ports["P11"])],
                RequestType.Z2: [("localhost", p1x_ports["P12"])],
                RequestType.Z3: [("localhost", p1x_ports["P13"])]
            },
            use_priority=True,
            use_round_robin=True
        )
        self.queues["Q1"] = q1
        
        # K1-2
        k1_port = self._allocate_port()
        client_k1 = ClientNode(
            node_id="K1",
            host="localhost",
            port=k1_port,
            queue_host="localhost",
            queue_port=q1_port,
            allowed_request_types=[RequestType.Z1, RequestType.Z2],
            metrics=self.metrics
        )
        self.clients.append(client_k1)
        
        k2_port = self._allocate_port()
        client_k2 = ClientNode(
            node_id="K2",
            host="localhost",
            port=k2_port,
            queue_host="localhost",
            queue_port=q1_port,
            allowed_request_types=[RequestType.Z2, RequestType.Z3],
            metrics=self.metrics
        )
        self.clients.append(client_k2)
        
        # Scaling 
        self.scaling_monitor = ScalingMonitor(
            get_queue_metrics=lambda: self.queues["Q1"].get_queue_metrics(),
            scale_up_callback=self._scale_up_processor,
            scale_down_callback=self._scale_down_processor
        )
        
        logger.info("Simulation setup complete")
    
    def _scale_up_processor(self, req_type: RequestType) -> None:
        type_num = req_type.value
        existing = [p for p in self.processors if f"P1{type_num}" in p]
        new_idx = len(existing) + 1
        new_id = f"P1{type_num}_{new_idx}"
        
        port = self._allocate_port()
        
        d_port = self.distributor.port
        
        new_processor = ProcessorNode(
            node_id=new_id,
            host="localhost",
            port=port,
            forward_to=[("localhost", d_port)],
            can_fail=False
        )
        
        new_processor.start()
        self.processors[new_id] = new_processor
        
        self.queues["Q1"].add_processor(req_type, "localhost", port)
        
        logger.info(f"Scaled up: added {new_id} at port {port}")
    
    def _scale_down_processor(self, req_type: RequestType) -> None:
        type_num = req_type.value
        existing = [p_id for p_id in self.processors if f"P1{type_num}_" in p_id]
        
        if not existing:
            logger.warning(f"Cannot scale down {req_type.name}: no extra processors to remove")
            return
        
        to_remove = existing[-1]
        processor = self.processors[to_remove]
        
        processor.stop()
        
        del self.processors[to_remove]
        
        self.queues["Q1"].remove_processor(req_type, processor.port)
        
        logger.info(f"Scaled down: removed {to_remove}")
    
    def start(self) -> None:
        logger.info("Starting simulation...")
     
        for proc_id in ["P21", "P22", "P23"]:
            self.processors[proc_id].start()
            time.sleep(0.1)
        
        for queue_id in ["Q21", "Q22", "Q23"]:
            self.queues[queue_id].start()
            time.sleep(0.1)
        
        self.distributor.start()
        time.sleep(0.1)
        
        for proc_id in ["P11", "P12", "P13"]:
            self.processors[proc_id].start()
            time.sleep(0.1)
        
        self.queues["Q1"].start()
        time.sleep(0.1)
        
        for client in self.clients:
            client.start()
            time.sleep(0.1)
        
        self.scaling_monitor.start()
        
        logger.info("All nodes started")
    
    def run(self, duration: float = None) -> None:
        duration = duration or CONFIG.simulation_duration
        logger.info(f"Running simulation for {duration} seconds...")
        
        start_time = time.time()
        status_interval = 10.0
        last_status = start_time
        
        while time.time() - start_time < duration:
            current_time = time.time()
            if current_time - last_status >= status_interval:
                self._log_status()
                last_status = current_time
            time.sleep(1.0)
        
        logger.info("Simulation duration complete")
    
    def _log_status(self) -> None:
        pending_by_client = {}
        for client in self.clients:
            stats = client.get_stats()
            pending_by_client[stats["node_id"]] = stats["pending"]
        
        summary = self.metrics.get_summary(pending_by_client)
        logger.info(f"Status: sent={summary["total_requests_sent"]}, "
                   f"success={summary["total_successful"]}, "
                   f"failed={summary["total_failed_permanently"]}, "
                   f"retried={summary["total_retried"]}, "
                   f"pending={summary["total_pending"]}, "
                   f"completed_rate={summary["success_rate_of_completed"]:.2%}")
    
    def stop(self) -> None:
        logger.info("Stopping simulation...")
        
        for client in self.clients:
            client.stop()
        
        self.scaling_monitor.stop()
        
        self.queues["Q1"].stop()
        
        for proc_id in ["P11", "P12", "P13"]:
            if proc_id in self.processors:
                self.processors[proc_id].stop()
        
        for proc_id in list(self.processors.keys()):
            if proc_id.startswith("P1") and "_" in proc_id:
                self.processors[proc_id].stop()
        
        if self.distributor:
            self.distributor.stop()
        
        for queue_id in ["Q21", "Q22", "Q23"]:
            self.queues[queue_id].stop()
        
        for proc_id in ["P21", "P22", "P23"]:
            self.processors[proc_id].stop()
        
        logger.info("Simulation stopped")
    
    def get_results(self) -> Dict:
        pending_by_client = {}
        for client in self.clients:
            stats = client.get_stats()
            pending_by_client[stats["node_id"]] = stats["pending"]
        
        results = {
            "metrics_summary": self.metrics.get_summary(pending_by_client),
            "client_stats": [c.get_stats() for c in self.clients],
            "processor_stats": {k: v.get_stats() for k, v in self.processors.items()},
            "queue_stats": {k: v.get_queue_metrics() for k, v in self.queues.items()},
            "scaling_status": self.scaling_monitor.get_status() if self.scaling_monitor else {}
        }
        return results