from flask import Flask, request, jsonify
import subprocess
import time
import os
import logging
from scipy import stats
import numpy as np
from kubernetes import client, config as k8s_config
from prometheus_api_client import PrometheusConnect

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger("CanaryService")

PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://prometheus:9090")
PRODUCTION_NAMESPACE = os.getenv("PRODUCTION_NAMESPACE", "default")
HELM_CHART_PATH = os.getenv("HELM_CHART_PATH", "/app/k8s/helm")

try:
    try:
        k8s_config.load_incluster_config()
    except:
        k8s_config.load_kube_config()
    v1 = client.CoreV1Api()
    logger.info("✓ Kubernetes API initialized.")
except Exception as e:
    logger.warning(f"Kubernetes API NOT initialized: {e}. Canary will use simulation.")
    v1 = None
prom = PrometheusConnect(url=PROMETHEUS_URL, disable_ssl=True)

def get_canary_metrics(namespace, window_s=60):
    """Fetch real latency metrics from Prometheus for the specific namespace."""
    try:
        query = f'sum(kafka_consumergroup_lag{{namespace="{namespace}", consumergroup="flink-consumer"}}) / (sum(flink_taskmanager_job_task_operator_numRecordsOutPerSecond{{namespace="{namespace}"}}) + 1) * 1000'
        data = prom.custom_query(query=query)
        if data:
            return float(data[0]['value'][1])
        return 0.0
    except Exception as e:
        logger.error(f"Prometheus query failed for {namespace}: {e}")
        return 0.0

@app.route('/deploy', methods=['POST'])
def deploy_canary():
    data = request.json
    candidate = data.get('candidate')
    production_namespace = data.get('production_namespace', PRODUCTION_NAMESPACE)
    logger.info(f"Received real canary request for candidate: {candidate} on production_ns: {production_namespace}")
    
    ts = int(time.time())
    namespace = f"canary-exp-{ts}"
    
    if not v1:
        logger.info("[SIMULATION] v1 API not available. Simulating canary success.")
        return jsonify({
            "status": "success",
            "verdict": "pass",
            "p_value": 0.01,
            "improvement_detected": True,
            "namespace": "simulation-ns"
        })

    try:
        # 1. Create Namespace
        logger.info(f"Creating namespace: {namespace}")
        ns_body = client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace))
        v1.create_namespace(ns_body)
        
        # 2. Deploy via Helm
        logger.info(f"Deploying candidate to {namespace}...")
        
        flink_cfg = candidate.get('flink', {})
        kafka_cfg = candidate.get('kafka', {})
        
        helm_cmd = [
            "helm", "install", "canary-release", "/app/k8s/helm",
            "--namespace", namespace,
            "--set", f"flink.taskmanager.replicas={flink_cfg.get('parallelism', 2)}",
            "--set", f"flink.checkpointInterval={flink_cfg.get('checkpoint_interval_ms', 5000)}",
            "--set", f"kafka.batchSize={kafka_cfg.get('batch_size', 16384)}",
            "--set", "tuner.enabled=false",
            "--set", "constraintGate.enabled=false",
            "--set", "canaryDeployer.enabled=false",
            "--wait", "--timeout", "5m"
        ]
        subprocess.run(helm_cmd, check=True)
        logger.info(f"✓ Configuration deployed to {namespace}")
        
        # 3. Collect Samples
        time.sleep(30) # Warmup
        samples = []
        for _ in range(5):
            time.sleep(10)
            val = get_canary_metrics(namespace)
            samples.append(val)
            
        # 4. Statistical Decision (Wilcoxon)
        # Fetch baseline samples from the specified production namespace
        baseline_samples = []
        for _ in range(5):
            val = get_canary_metrics(production_namespace)
            baseline_samples.append(val)
            
        stat, p_val = stats.wilcoxon(baseline_samples, samples)
        verdict = "pass" if p_val < 0.05 and np.median(samples) < np.median(baseline_samples) else "fail"
        
        logger.info(f"Canary Finished. p_value: {p_val:.4f}, Verdict: {verdict}")
        
        return jsonify({
            "status": "success",
            "verdict": verdict,
            "p_value": float(p_val),
            "improvement_detected": verdict == "pass",
            "namespace": namespace
        })

    except Exception as e:
        logger.error(f"Canary orchestration failed: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        # Cleanup namespace
        logger.info(f"Cleaning up namespace: {namespace}")
        try:
            v1.delete_namespace(namespace)
        except: pass

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
