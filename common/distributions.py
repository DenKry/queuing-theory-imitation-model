
import math
import random

from config import CONFIG, ServiceTimeType


class ServiceTimeGenerator:
    @staticmethod
    def generate() -> float:

        if CONFIG.service_time_type == ServiceTimeType.FIXED:
            return CONFIG.service_time_fixed
        
        elif CONFIG.service_time_type == ServiceTimeType.UNIFORM:
            a = CONFIG.service_time_uniform_a
            b = CONFIG.service_time_uniform_b
            return random.uniform(a, b)
        
        elif CONFIG.service_time_type == ServiceTimeType.EXPONENTIAL:
            lambda_param = CONFIG.service_time_exp_lambda
            chi = random.random()
            while chi == 0:
                chi = random.random()
            return (-1.0 / lambda_param) * math.log(chi)
        
        elif CONFIG.service_time_type == ServiceTimeType.NORMAL:
            chi_1 = random.random()
            chi_2 = random.random()
            while chi_1 == 0:
                chi_1 = random.random()
            
            mean = CONFIG.service_time_normal_mean
            std = CONFIG.service_time_normal_std
            
            z = math.sqrt(-2.0 * math.log(chi_1)) * math.cos(2.0 * math.pi * chi_2)
            return max(0.01, mean+ std * z)
        
        else:
            return CONFIG.service_time_fixed
