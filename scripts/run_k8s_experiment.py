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
HELM_DIR = ROOT_DIR / "k8s/helm"

# Detect Helm
LOCAL_HELM = ROOT_DIR / "scripts/windows-amd64/helm.exe"
HELM_CMD = str(LOCAL_HELM) if LOCAL_HELM.exists() else "helm"
logger.info(f"Using Helm: {HELM_CMD}")

def run_command(cmd, cwd=None, env=None, capture=False):
    # Replace 'helm' with HELM_CMD if it starts with helm
    if cmd.startswith("helm "):
        cmd = cmd.replace("helm ", f"{HELM_CMD} ", 1)
        
    logger.info(f"Running: {cmd}")
    if capture:
        return subprocess.check_output(cmd, shell=True, cwd=cwd or ROOT_DIR, env=env).decode('utf-8')
    else:
        subprocess.run(cmd, shell=True, check=True, cwd=cwd or ROOT_DIR, env=env)

def check_minikube_context():
    """Enforce Minikube context - fail fast if not in Minikube."""
    try:
        current = subprocess.check_output("kubectl config current-context", shell=True).decode().strip()
        if current != "minikube":
            logger.error(f"FATAL: Not running in Minikube context (current: {current})")
            logger.error("This script MUST run in Minikube. Switch with: kubectl config use-context minikube")
            raise RuntimeError(f"Wrong Kubernetes context: {current}. Expected: minikube")
        logger.info("✓ Verified Minikube context")
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to get kubectl context: {e}")
        raise

def main():
    check_minikube_context()
    parser = argparse.ArgumentParser(description="Run experiment on Minikube")
    parser.add_argument("scenario", help="Scenario ID (e.g. S1_steady)")
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--build", action="store_true", help="Build and load images into Minikube")
    parser.add_argument("--variant", choices=["baseline", "tuner"], default="tuner", help="Experiment variant")
    parser.add_argument("--run-name", help="Custom run name for artifacts")
    parser.add_argument("--seed-gen", type=int, default=1234)
    parser.add_argument("--seed-opt", type=int, default=2345)
    parser.add_argument("--seed-fail", type=int, default=3456)
    parser.add_argument("--force-image-push", action="store_true", help="Force push images to Minikube before deployment")
    parser.add_argument("--namespace", help="Target namespace")
    args = parser.parse_args()

    scenario_id = args.scenario
    variant = args.variant
    run_name = args.run_name if args.run_name else f"{scenario_id}_{variant}_run{args.repeats}"
    
    # NAMESPACE MANAGEMENT: Delete if exists, then create
    namespace = args.namespace if args.namespace else f"exp-{run_name.lower().replace('_', '-')}"
    logger.info(f"Managing namespace: {namespace}")
    
    # Delete namespace if it exists (idempotent cleanup)
    try:
        logger.info(f"Checking if namespace {namespace} exists...")
        result = subprocess.run(f"kubectl get namespace {namespace}", shell=True, capture_output=True)
        if result.returncode == 0:
            logger.info(f"Namespace {namespace} exists, deleting...")
            run_command(f"kubectl delete namespace {namespace}")
            time.sleep(10)  # Wait for full cleanup
            logger.info(f"✓ Namespace {namespace} deleted")
        else:
            logger.info(f"Namespace {namespace} does not exist (clean start)")
    except Exception as e:
        logger.warning(f"Error checking/deleting namespace: {e}")
    
    # Create namespace
    logger.info(f"Creating namespace: {namespace}")
    run_command(f"kubectl create namespace {namespace}")
    
    scenario_path = CONFIG_DIR / f"{scenario_id}.yaml"
    
    if not scenario_path.exists():
        logger.error(f"Scenario {scenario_path} not found")
        run_command(f"kubectl delete namespace {namespace}")
        return

    with open(scenario_path, "r") as f:
        config = yaml.safe_load(f)
    
    duration = config.get("duration_sec", 60)
    rate_cfg = config.get("rate_control", {})
    target_rate = rate_cfg.get("target_rate_events_per_sec", rate_cfg.get("initial_rate_events_per_sec", 50.0))

    # Images are already in Minikube (pushed by orchestrator)

    # 2. Prepare ConfigMap for Scenario (in namespace)
    logger.info("Creating Scenario ConfigMap...")
    run_command(f"kubectl create configmap scenario-config --from-file=current_scenario.yaml={scenario_path} --dry-run=client -o yaml | kubectl apply -n {namespace} -f -")

    # 3. Install Helm Chart (in namespace)
    logger.info(f"Installing Helm Chart in namespace {namespace}...")
    overrides = (
        f"--set flink.image=docker.io/library/code-jobmanager:latest "
        f"--set workloadGenerator.image=docker.io/library/code-workload-generator:latest "
        f"--set tuner.image=docker.io/library/code-tuner:v5 "
        f"--set constraintGate.image=docker.io/library/code-constraint-gate:latest "
        f"--set canaryDeployer.image=docker.io/library/code-canary-deployer:v4 "
        f"--set rolloutManager.image=docker.io/library/code-rollout-manager:latest "
        f"--set tuner.rolloutManagerUrl=http://rollout-manager:8000 "
        f"--set canaryDeployer.productionNamespace={namespace} "
        f"--set workloadGenerator.targetRate={target_rate} "
        f"--set tuner.namespace={namespace} "
        f"--set workloadGenerator.seed={args.seed_gen} "
        f"--set tuner.seed={args.seed_opt} "
        f"--set tuner.enabled={'true' if args.variant == 'tuner' else 'false'} "
        f"--set tuner.baselineMode={'true' if args.variant == 'baseline' else 'false'} "
    )
    
    run_command(f"helm install autotuning-framework {HELM_DIR} {overrides} --namespace {namespace} --wait --timeout 5m")
    
    # Wait for all pods to be ready
    logger.info("Waiting for all pods to be ready...")
    try:
        # Wait for all pods in namespace to be ready (5 minute timeout)
        run_command(f"kubectl wait --for=condition=ready pod --all -n {namespace} --timeout=300s")
        logger.info("✓ All pods are ready")
    except Exception as e:
        logger.warning(f"Some pods may not be ready yet: {e}")
        # Continue anyway - specific readiness checks below will catch issues
    
    # 4. Wait for Kafka and DNS to be ready
    logger.info("Waiting for Kafka connectivity and DNS...")
    for i in range(20):
        try:
            jm_pod = run_command(f"kubectl get pods -n {namespace} -l component=jobmanager -o jsonpath='{{.items[0].metadata.name}}'", capture=True).strip().strip("'")
            run_command(f"kubectl exec -n {namespace} {jm_pod} -- python3 -c \"import socket; socket.gethostbyname('kafka')\"", capture=True)
            run_command(f"kubectl exec -n {namespace} {jm_pod} -- python3 -c \"import socket; socket.create_connection(('kafka', 9092))\"", capture=True)
            logger.info("Kafka is reachable (DNS + TCP).")
            break
        except:
            logger.info(f"Kafka not ready yet (attempt {i+1}/20)...")
            time.sleep(5)
    else:
        logger.error("Kafka DNS failed to become ready.")
        run_command(f"kubectl delete namespace {namespace}")
        raise Exception("Kafka DNS failure")

    # 5. Create Kafka Topics (input and output)
    logger.info("Creating Kafka topics...")
    try:
        kafka_pod = run_command(f"kubectl get pods -n {namespace} -l app=kafka -o jsonpath='{{.items[0].metadata.name}}'", capture=True).strip().strip("'")
        # Input topic
        run_command(f"kubectl exec -n {namespace} {kafka_pod} -- kafka-topics --create --topic trips_in --bootstrap-server localhost:9092 --partitions 2 --replication-factor 1 --if-not-exists")
        logger.info("✓ Created/verified topic: trips_in")
        # Output topic (prevent Flink restart loop)
        run_command(f"kubectl exec -n {namespace} {kafka_pod} -- kafka-topics --create --topic trips_stats --bootstrap-server localhost:9092 --partitions 2 --replication-factor 1 --if-not-exists")
        logger.info("✓ Created/verified topic: trips_stats")
    except Exception as e:
        logger.warning(f"Failed to create topics: {e}")

    logger.info("Submitting Flink Job...")
    jm_pod = run_command(f"kubectl get pods -n {namespace} -l component=jobmanager -o jsonpath='{{.items[0].metadata.name}}'", capture=True).strip().strip("'")
    run_command(f"kubectl exec -n {namespace} {jm_pod} -- ./bin/flink run -py /opt/flink/usrlib/job.py -d")
    
    # 6. Wait for Flink Job Readiness
    logger.info("Waiting for Flink Job to start...")
    
    # Clean up any existing port-forward on 8081
    try:
        if os.name == 'nt':  # Windows
            subprocess.run("for /f \"tokens=5\" %a in ('netstat -aon ^| findstr :8081') do taskkill /F /PID %a", shell=True, capture_output=True)
        else:  # Linux/Mac
            subprocess.run("lsof -ti:8081 | xargs kill -9", shell=True, capture_output=True)
        time.sleep(1)
    except:
        pass
    
    pf_check = subprocess.Popen(["kubectl", "port-forward", f"pod/{jm_pod}", "8081:8081", "-n", namespace], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(3)
    
    try:
        max_retries = 30 # 30 * 5s = 150s timeout for job start
        ready = False
        import requests
        
        for i in range(max_retries):
            try:
                # Check Jobs
                resp = requests.get("http://localhost:8081/jobs/overview").json()
                running_jobs = [j for j in resp.get("jobs", []) if j["state"] == "RUNNING"]
                
                # Check TaskManagers/Slots
                tm_resp = requests.get("http://localhost:8081/taskmanagers").json()
                total_slots = sum(tm.get("slotsNumber", 0) for tm in tm_resp.get("taskmanagers", []))
                
                if running_jobs and total_slots >= 1:
                    logger.info(f"System Ready: {len(running_jobs)} running job(s), {total_slots} slots.")
                    ready = True
                    break
                else:
                    logger.info(f"Waiting for Job/Slots... (Jobs: {len(running_jobs)}, Slots: {total_slots})")
            except Exception as e:
                logger.debug(f"Readiness check connection failed: {e}")
                
            time.sleep(5)
            
        if not ready:
             logger.warning("Timed out waiting for Flink Job/Slots. Proceeding anyway (might affect baseline).")
             
    except Exception as e:
        logger.error(f"Readiness check failed with error: {e}")
    finally:
        pf_check.terminate()

    logger.info(f"Experiment running for {duration} seconds...")
    time.sleep(duration)
    
    logger.info("Experiment duration reached.")

    # 7. Collect Artifacts
    run_name_final = run_name  # Already defined at the top
    output_dir = ROOT_DIR / "artifacts" / run_name
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Port forward Prometheus to localhost
    logger.info("Port-forwarding Prometheus...")
    pf_proc = subprocess.Popen(["kubectl", "port-forward", "svc/prometheus", "9090:9090", "-n", namespace], stdout=subprocess.DEVNULL)
    time.sleep(2)
    
    try:
        logger.info("Collecting artifacts...")
        collect_script = ROOT_DIR / "scripts/collect_artifacts.py"
        # Pass namespace to collector
        env = os.environ.copy()
        env["K8S_NAMESPACE"] = namespace
        run_command(f"python {collect_script} {run_name} {scenario_id} --k8s", env=env)
        
    finally:
        pf_proc.terminate()
        logger.info("Cleaning up namespace...")
        # DELETE NAMESPACE - this removes all resources automatically
        run_command(f"kubectl delete namespace {namespace}")
        logger.info(f"✓ Namespace {namespace} deleted successfully")

if __name__ == "__main__":
    main()
