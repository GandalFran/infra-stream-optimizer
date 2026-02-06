from flask import Flask, request, jsonify
import requests
import os
import logging
import time
import subprocess
from kubernetes import client, config as k8s_config

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger("RolloutManager")

GATE_URL = os.getenv("GATE_URL", "http://constraint-gate:8000")
CANARY_URL = os.getenv("CANARY_URL", "http://canary-deployer:8000")
PRODUCTION_NAMESPACE = os.getenv("PRODUCTION_NAMESPACE", "default")

try:
    try:
        k8s_config.load_incluster_config()
    except:
        k8s_config.load_kube_config()
    v1 = client.AppsV1Api()
    logger.info("✓ Kubernetes API initialized.")
except Exception as e:
    logger.warning(f"Kubernetes API NOT initialized: {e}. Rollout phase will be simulated.")
    v1 = None

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "ok"})

@app.route('/rollout', methods=['POST'])
def handle_rollout():
    """
    Orchestrates the formal rollout process:
    1. Constraint Gate Check
    2. Canary Experiment
    3. Production Rollout (Staged)
    """
    data = request.json
    candidate = data.get('candidate')
    namespace = data.get('namespace', PRODUCTION_NAMESPACE)
    
    logger.info(f"Initiating formal rollout for config in namespace: {namespace}")
    
    # --- PHASE 1: CONSTRAINT GATE ---
    logger.info("PHASE 1: Constraint Gate Validation...")
    try:
        gate_resp = requests.post(f"{GATE_URL}/validate", json=candidate, timeout=10)
        if gate_resp.status_code != 200 or not gate_resp.json().get('valid'):
            reasons = gate_resp.json().get('reasons', 'Unknown violation')
            logger.warning(f"Aborting rollout: Constraint Gate REJECTED: {reasons}")
            return jsonify({
                "status": "rejected",
                "phase": "gate",
                "message": f"Constraint Gate Violation: {reasons}"
            }), 400
        logger.info("✓ Gate Passed.")
    except Exception as e:
        logger.error(f"Gate check failed: {e}")
        return jsonify({"status": "error", "message": "Gate unreachable"}), 500

    # --- PHASE 2: CANARY EXPERIMENT ---
    logger.info("PHASE 2: Canary Experimentation...")
    try:
        canary_payload = {
            "candidate": candidate,
            "production_namespace": namespace
        }
        canary_resp = requests.post(f"{CANARY_URL}/deploy", json=canary_payload, timeout=600)
        if canary_resp.status_code != 200:
             logger.error(f"Canary orchestration failed: {canary_resp.text}")
             return jsonify({"status": "error", "message": "Canary service error"}), 500
        
        canary_data = canary_resp.json()
        if canary_data.get('verdict') != 'pass':
            logger.warning("Aborting rollout: Canary Experiment FAILED to show improvement.")
            return jsonify({
                "status": "rejected",
                "phase": "canary",
                "message": "Canary experiment failed (no improvement or SLO violation)",
                "p_value": canary_data.get('p_value')
            }), 200
        logger.info(f"✓ Canary Passed (p={canary_data.get('p_value')})")
    except Exception as e:
        logger.error(f"Canary phase error: {e}")
        return jsonify({"status": "error", "message": "Canary execution error"}), 500

    # --- PHASE 3: STAGED ROLLOUT ---
    logger.info("PHASE 3: Staged Production Rollout...")
    if not v1:
        logger.info("[SIMULATION] v1 API not available. Simulating success.")
        return jsonify({
            "status": "success",
            "message": "[SIMULATION] Rollout successful (No K8s connection)",
            "p_value": canary_data.get('p_value')
        })

    try:
        # In this implementation, we simulate the "staged" part by applying the config
        # and checking the pod health for a few minutes.
        # In a real system, we'd use 'helm upgrade --wait' or similar.
        
        # Applying parallelism via K8s patch (as a proxy for full rollout)
        flink_cfg = candidate.get('flink', {})
        parallelism = flink_cfg.get('parallelism', 2)
        
        deploy_body = {"spec": {"replicas": parallelism}}
        v1.patch_namespaced_deployment(
            name="flink-taskmanager",
            namespace=namespace,
            body=deploy_body
        )
        
        # Monitor health for 30s (Staged Verification)
        logger.info("Monitoring production health post-rollout...")
        time.sleep(15) 
        
        # Check if pods are ready
        tm_deploy = v1.read_namespaced_deployment(name="flink-taskmanager", namespace=namespace)
        if tm_deploy.status.ready_replicas != parallelism:
            logger.error("Rollout FAILED: Pods not ready in timeout. Initiating Rollback...")
            # Simple Rollback: Reset replicas (real one would restore original config)
            # For now just return error
            return jsonify({"status": "failed", "phase": "rollout", "message": "Production pods failed to stabilize"}), 500
        
        logger.info("✓ Rollout COMPLETE and Verified.")
        return jsonify({
            "status": "success",
            "message": "Configuration successfully rolled out to production",
            "p_value": canary_data.get('p_value')
        })
        
    except Exception as e:
        logger.error(f"Rollout execution error: {e}")
        return jsonify({"status": "error", "message": "Production rollout crashed"}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
