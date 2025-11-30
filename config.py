
from dataclasses import dataclass
from enum import Enum


class QueueDiscipline(Enum):
    FIFO = "fifo"


class ServiceTimeType(Enum):
    FIXED = "fixed"
    UNIFORM = "uniform"
    EXPONENTIAL = "exponential"
    NORMAL = "normal"


class RemovalMode(Enum):
    AFTER_SERVICE = "after_service"


@dataclass
class Config:
    # Queue
    queue_discipline: QueueDiscipline = QueueDiscipline.FIFO
    priority_enabled: bool = True
    removal_mode: RemovalMode = RemovalMode.AFTER_SERVICE
    
    # Service time (type)
    service_time_type: ServiceTimeType = ServiceTimeType.EXPONENTIAL    # or FIXED, NORMAL, EXPONENTIAL, UNIFORM
    service_time_fixed: float = 1.0
    service_time_uniform_a: float = 0.5
    service_time_uniform_b: float = 2.0
    service_time_exp_lambda: float = 1.0
    service_time_normal_mean: float = 1.0
    service_time_normal_std: float = 0.2
    
    # Dynamic scaling
    avg_wait_time_threshold: float = 5.0
    scale_down_threshold: float = 1.5
    scaling_cooldown: float = 10.0
    max_processors_per_type: int = 5
    min_processors_per_type: int = 1
    
    # Fault tolerance
    p2x_failure_probability: float = 0.025
    idle_timeout: float = 60.0
    client_request_timeout: float = 15.0
    max_retries: int = 2 
    
    # Network
    tcp_port_base: int = 5000
    buffer_size: int = 4096
    
    # Simulation
    simulation_duration: float = 60.0
    request_generation_rate: float = 2.0  # req/sec
    random_seed: int = 326

CONFIG = Config()