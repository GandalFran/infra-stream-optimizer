import argparse
import yaml
import time
import subprocess
import os
import shutil
import logging
import json
import datetime
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).parent.parent
CONFIG_DIR = ROOT_DIR / "configs/scenarios"

def run_command(cmd, cwd=None, env=None):
    logger.info(f"Running: {cmd}")
    subprocess.run(cmd, shell=True, check=True, cwd=cwd or ROOT_DIR, env=env)

def main():
    parser = argparse.ArgumentParser(description="Run a local experiment scenario using Docker Compose")
    parser.add_argument("scenario", help="Scenario ID (e.g. S1_steady) or path to yaml config")
    parser.add_argument("--repeats", type=int, default=1, help="Number of times to repeat the experiment")
    parser.add_argument("--variant", default="tuner", choices=["baseline", "tuner"], help="System variant to run")
    parser.add_argument("--run-name", help="Custom run name (overrides auto-generated)")
    args = parser.parse_args()

    # Resolve scenario path
    if args.scenario.endswith(".yaml"):
        scenario_path = Path(args.scenario)
        scenario_id = scenario_path.stem
    else:
        scenario_id = args.scenario
        scenario_path = CONFIG_DIR / f"{scenario_id}.yaml"

    if not scenario_path.exists():
        logger.error(f"Scenario configuration not found at {scenario_path}")
        return

    logger.info(f"Loading scenario: {scenario_id} from {scenario_path}. Variant: {args.variant}. Repeats: {args.repeats}")
    
    with open(scenario_path, "r") as f:
        config = yaml.safe_load(f)
    
    duration = config.get("duration_sec", 60)
    topic = config.get("kafka", {}).get("topic", "trips_in")
    
    # Prepare Environment for Docker Compose
    env = os.environ.copy()
    env["SCENARIO_PATH"] = f"/app/configs/scenarios/{scenario_path.name}"
    env["WORKLOAD_GENERATOR_CONFIG"] = f"/app/configs/scenarios/{scenario_path.name}"
    
    target_rate = config.get("rate_control", {}).get("target_rate_events_per_sec")
    if target_rate:
        env["TARGET_RATE"] = str(target_rate)
        logger.info(f"Setting TARGET_RATE={target_rate}")

    for i in range(args.repeats):
        run_idx = i + 1
        run_id_label = args.run_name or f"{scenario_id}_run_{run_idx}"
        logger.info(f"--- Starting Run {run_idx}/{args.repeats} [{run_id_label}] ---")
        
        # Define services based on variant
        # Base services
        services = ["zookeeper", "kafka", "kafka-exporter", "jobmanager", "taskmanager", "constraint-gate", "workload-generator", "prometheus"]
        
        # Variant specific: default is Tuner
        # If baseline, we assume Tuner is NOT controlling, or we use a "baseline" tuner implementation?
        # Protocol says: variant: baseline vs proposed.
        # If 'baseline', we might still run the Tuner but in 'observation' mode? Or just omit it?
        # If we omit Tuner, Flink job needs to run without tuner connectivity?
        # Flink connects to JobManager. Tuner uses Flink metrics.
        # Tuner is external. So omitting it is safe for Baseline (no control loop).
        
        if args.variant == "tuner":
            services.append("tuner")
            
        try:
            logger.info(f"Starting Docker Compose stack for {args.variant}...")
            # Make sure to remove orphans to clean up 'tuner' if switching variants
            run_command(f"docker-compose down --remove-orphans", env=env)
            
            # Start selected services
            svc_str = " ".join(services)
            run_command(f"docker-compose up -d {svc_str}", env=env)

            # NEW: Submit Flink Job
            logger.info("Submitting Flink Job...")
            # Allow some time for JobManager to become ready
            time.sleep(10)
            
            # Use docker-compose exec to submit
            # -d for detached (Flink job runs in cluster)
            submit_cmd = "docker-compose exec -T jobmanager ./bin/flink run -py /opt/flink/usrlib/job.py -d"
            try:
                run_command(submit_cmd, env=env)
                logger.info("Flink Job Submitted Successfully.")
            except subprocess.CalledProcessError as e:
                logger.error(f"Failed to submit Flink job: {e}")
                raise e

            logger.info(f"Experiment running. Waiting for {duration} seconds...")
            # Optional: Poll for job running status?
            # For now, just wait duration.
            time.sleep(duration)
            
            logger.info("Experiment duration reached.")
            
            # Prepare Artifact Directory
            run_output_dir = ROOT_DIR / f"artifacts/{run_id_label}"
            os.makedirs(run_output_dir, exist_ok=True)

            # Write run_meta.json explicitly
            meta = {
                "run_id": run_id_label,
                "scenario_id": scenario_id,
                "variant": args.variant,
                "timestamp_utc": datetime.datetime.utcnow().isoformat(),
                "services_started": services,
                "config_snapshot": config,
                "job_submission": "explicit" 
            }
            with open(run_output_dir / "run_meta.json", "w") as f:
                json.dump(meta, f, indent=2)

            # Trigger artifact collection
            logger.info("Collecting artifacts...")
            collect_script = ROOT_DIR / "scripts/collect_artifacts.py"
            
            # Pass unique run ID to collector
            run_command(f"python {collect_script} {run_id_label} {scenario_id}", env=env)
            
            # Verify artifact existence
            result_file = run_output_dir / f"{scenario_id}_result.json"
            if result_file.exists():
                logger.info(f"Artifacts saved to {run_output_dir}")
                logger.info("DEBUG: Check metrics_timeseries.json content to verify data.")
            else:
                logger.warning(f"Artifacts for {run_id_label} missing! {result_file} not found.")
                
        except KeyboardInterrupt:
            logger.info("Interrupted by user.")
            break
        except Exception as e:
            logger.error(f"Experiment run {run_idx} failed: {e}")
            import traceback
            traceback.print_exc()
        finally:
            logger.info("Stopping services...")
            # run_command("docker-compose down") # Commented out for debug if needed, but per protocol we shut down
            run_command("docker-compose down", env=env)
            time.sleep(5)

if __name__ == "__main__":
    main()
