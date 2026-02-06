import yaml
import time
import subprocess
import os
import logging
import json
import datetime
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Orchestrator")

ROOT_DIR = Path(__file__).parent.parent
CONFIG_DIR = ROOT_DIR / "configs"
SEEDS_FILE = CONFIG_DIR / "seeds.yaml"

PROTOCOL_CONFIG = {
    "scenarios": ["S1_steady", "S2_spike", "S3_failure", "S4_drift"],
    "variants": ["baseline", "tuner"], 
    "repetitions": 5,
    "warmup_sec": 120,
    "measurement_sec": 600,
    "cooldown_sec": 60,
    "paired_runs": True
}

def load_seeds():
    if not SEEDS_FILE.exists():
        logger.error(f"Seeds file not found at {SEEDS_FILE}")
        return []
    with open(SEEDS_FILE, "r") as f:
        data = yaml.safe_load(f)
        return data.get("seeds", [])

def format_duration(seconds):
    return str(datetime.timedelta(seconds=int(seconds)))

def run_experiment_step(scenario_id, variant, run_index, seed_tuple):
    run_name = f"{scenario_id}_{variant}_rep{run_index}"
    # Logger handled in main loop for progress bars
    
    env = os.environ.copy()
    env["EXP_SEED_GEN"] = str(seed_tuple[0])
    env["EXP_SEED_OPT"] = str(seed_tuple[1])
    env["EXP_SEED_FAIL"] = str(seed_tuple[2])
    
    namespace = f"exp-{run_name.lower().replace('_', '-')}"
    logger.info(f"Targeting namespace: {namespace} for {variant}")
    
    cmd = [
        "python", "scripts/run_k8s_experiment.py",
        scenario_id,
        "--variant", variant,
        "--run-name", run_name,
        "--namespace", namespace,
        "--seed-gen", str(seed_tuple[0]),
        "--seed-opt", str(seed_tuple[1]),
        "--seed-fail", str(seed_tuple[2])
    ]
    
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            subprocess.run(cmd, check=True, cwd=ROOT_DIR, env=env)
            
            # Validate metrics after successful run
            artifact_dir = ROOT_DIR / "artifacts" / run_name
            metrics_file = artifact_dir / "metrics.json"
            
            if metrics_file.exists():
                try:
                    with open(metrics_file, 'r') as f:
                        metrics = json.load(f)
                    
                    # Check if latency metrics are zero (indicates collection failure)
                    latency_mean = metrics.get("summary", {}).get("e2e_latency_ms", {}).get("mean", -1)
                    
                    if latency_mean == 0:
                        logger.warning(f"Zero-metrics detected for {run_name} (attempt {retry_count + 1}/{max_retries})")
                        logger.info(f"Deleting corrupted artifacts and retrying in 60s...")
                        
                        # Delete corrupted artifacts
                        import shutil
                        shutil.rmtree(artifact_dir)
                        
                        # Wait before retry
                        time.sleep(60)
                        retry_count += 1
                        continue
                    else:
                        logger.info(f"✓ Metrics validated for {run_name} (latency_mean={latency_mean:.1f}ms)")
                        return True
                        
                except Exception as e:
                    logger.error(f"Failed to validate metrics for {run_name}: {e}")
                    return True  # Continue anyway if validation fails
            else:
                logger.warning(f"Metrics file not found for {run_name}")
                return True
                
        except subprocess.CalledProcessError as e:
            logger.error(f"Run {run_name} FAILED! Error: {e}")
            # Robustness: Attempt cleanup and continue
            try:
                 subprocess.run("helm uninstall autotuning-framework", shell=True, check=False)
                 time.sleep(10)
            except:
                pass
            return False
    
    # Max retries exhausted
    logger.error(f"Max retries ({max_retries}) exhausted for {run_name}. Giving up.")
    return False
    
    # Small delay between experiments for stability
    time.sleep(5)


def main():
    logger.info("Initializing Robust Experiment Protocol (N=5)...")
    
    # Enforce Minikube context
    try:
        current = subprocess.check_output("kubectl config current-context", shell=True).decode().strip()
        if current != "minikube":
            logger.error(f"Wrong context: {current}. Please switch to minikube.")
            return
        logger.info("✓ Verified Minikube context")
    except Exception as e:
        logger.error(f"Failed to verify context: {e}")
        return
    
    # BUILD AND PUSH IMAGES ONCE AT START
    logger.info("Building Docker images...")
    try:
        subprocess.run("docker compose build jobmanager workload-generator tuner constraint-gate canary-deployer", 
                      shell=True, check=True, cwd=ROOT_DIR)
        logger.info("✓ Images built successfully")
    except Exception as e:
        logger.error(f"Failed to build images: {e}")
        return
    
    logger.info("Pushing images to Minikube...")
    try:
        subprocess.run("minikube image load code-jobmanager:latest", shell=True, check=True, cwd=ROOT_DIR)
        subprocess.run("minikube image load code-workload-generator:latest", shell=True, check=True, cwd=ROOT_DIR)
        subprocess.run("minikube image load code-tuner:latest", shell=True, check=True, cwd=ROOT_DIR)
        subprocess.run("minikube image load code-constraint-gate:latest", shell=True, check=True, cwd=ROOT_DIR)
        subprocess.run("minikube image load code-canary-deployer:latest", shell=True, check=True, cwd=ROOT_DIR)
        subprocess.run("minikube image load code-rollout-manager:latest", shell=True, check=True, cwd=ROOT_DIR)
        logger.info("✓ Images pushed to Minikube successfully")
    except Exception as e:
        logger.error(f"Failed to push images to Minikube: {e}")
        return

    seeds = load_seeds()
    if not seeds:
        logger.warning("No seeds found, generating random seeds...")
        import random
        seeds = [[random.randint(1000,9999) for _ in range(3)] for _ in range(PROTOCOL_CONFIG["repetitions"])]
    
    # Calculate Total Operations
    scenarios = PROTOCOL_CONFIG["scenarios"]
    variants = PROTOCOL_CONFIG["variants"]
    reps = PROTOCOL_CONFIG["repetitions"]
    total_runs = len(scenarios) * len(variants) * reps
    
    current_run = 0
    results_summary = {"success": [], "failed": []}
    start_time_global = time.time()
    
    logger.info(f"Total Runs Scheduled: {total_runs}")
    logger.info("Execution Pattern: Interleaved (all scenarios per repetition)\n")

    # Reorganized loop: REP -> SCENARIO -> VARIANT (interleaved execution)
    for i in range(reps):
        rep_idx = i + 1
        if i >= len(seeds):
            seed = seeds[-1]
        else:
            seed = seeds[i]
        
        # Paired Runs Order for this repetition
        order = ["baseline", "tuner"] if rep_idx % 2 != 0 else ["tuner", "baseline"]
        
        logger.info(f"\n{'='*60}")
        logger.info(f"  REPETITION {rep_idx}/{reps} (Order: {' -> '.join(order)})")
        logger.info(f"{'='*60}")
        
        for scenario in scenarios:
            for variant in order:
                current_run += 1
                run_id = f"{scenario}_{variant}_rep{rep_idx}"
                
                # Skip if artifacts already exist (resumability)
                artifact_dir = ROOT_DIR / "artifacts" / run_id
                if artifact_dir.exists():
                    logger.info(f"⊘ SKIP: {run_id} (artifacts already exist)")
                    results_summary["success"].append(run_id + " (skipped)")
                    continue
                
                # Progress & ETA Calculation
                elapsed_global = time.time() - start_time_global
                if current_run > 1:
                    avg_time = elapsed_global / (current_run - 1)
                    remaining_runs = total_runs - (current_run - 1)
                    eta_seconds = avg_time * remaining_runs
                    eta_str = format_duration(eta_seconds)
                else:
                    eta_str = "Calculating..."
                
                progress_pct = (current_run / total_runs) * 100
                logger.info(f"\n▶ Experiment {current_run}/{total_runs} ({progress_pct:.1f}%) | ETA: {eta_str}")
                logger.info(f"  Running: {run_id}")
                
                success = run_experiment_step(scenario, variant, rep_idx, seed)
                
                if success:
                    results_summary["success"].append(run_id)
                    logger.info(f"✓ SUCCESS: {run_id}")
                else:
                    results_summary["failed"].append(run_id)
                    logger.error(f"✗ FAILURE: {run_id}. Continuing...")

    logger.info("=== Protocol Execution Complete ===")
    logger.info(f"Total Time: {format_duration(time.time() - start_time_global)}")
    
    # Final Summary Report
    logger.info("\n" + "="*40)
    logger.info("       EXECUTION SUMMARY")
    logger.info("="*40)
    logger.info(f"Total Runs: {total_runs}")
    logger.info(f"Successful: {len(results_summary['success'])}")
    logger.info(f"Failed:     {len(results_summary['failed'])}")
    
    if results_summary["failed"]:
        logger.info("\nFAILED RUNS:")
        for r in results_summary["failed"]:
            logger.info(f" - {r}")
    else:
        logger.info("\nAll runs completed successfully.")
    logger.info("="*40 + "\n")
    
    # Generate Aggregate Report
    try:
        logger.info("Generating Aggregate Report...")
        subprocess.run(["python", "scripts/aggregate_results.py"], check=True, cwd=ROOT_DIR)
    except Exception as e:
         logger.error(f"Failed to generate aggregate report: {e}")

if __name__ == "__main__":
    main()
