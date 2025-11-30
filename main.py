
import argparse
import json

from simulation.simulation_engine import SimulationEngine
from common.logger import setup_logging, get_logger
from config import CONFIG


def main():
    parser = argparse.ArgumentParser(description="DSA LW2")
    parser.add_argument("--duration", type=float, default=60.0)
    parser.add_argument("--rate", type=float, default=2.0)
    parser.add_argument("--seed", type=int, default=325)
    args = parser.parse_args()
    
    setup_logging("INFO")
    logger = get_logger("main")
    
    CONFIG.simulation_duration = args.duration
    CONFIG.request_generation_rate = args.rate
    CONFIG.random_seed = args.seed

    engine = SimulationEngine()
    
    try:
        engine.setup()
        engine.start()
        engine.run()
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    finally:
        engine.stop()
        
        results = engine.get_results()
        logger.info("")
        logger.info("RESULTS")
        logger.info("")

        output_file = "simulation_results.json"
        with open(output_file, "w") as f:
            json.dump(results, indent=2, default=str, fp=f)
        logger.info(f"Results saved to {output_file}")
        
        summary = results["metrics_summary"]
        logger.info(f"Total requests: {summary["total_requests_sent"]}")
        logger.info(f"Successful: {summary["total_successful"]} ({summary["success_rate_overall"]:.1%})")
        logger.info(f"Failed: {summary["total_failed_permanently"]}")
        logger.info(f"Retried: {summary["total_retried"]}")
        logger.info(f"Average latency: {summary["average_latency"]:.2f}s")
        logger.info(f"Throughput: {summary["throughput"]:.2f} req/s")


if __name__ == "__main__":
    main()

# python main.py --duration 60 --rate 2.0 --seed 123

# python -m pytest tests/ -v