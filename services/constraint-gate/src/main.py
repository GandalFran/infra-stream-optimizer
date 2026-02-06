import logging
import os
from flask import Flask, request, jsonify
import yaml

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "ok"})

# Global state to track configuration magnitude
LAST_VALID_CONFIG = {}

def validate_config(config):
    global LAST_VALID_CONFIG
    reasons = []
    
    flink_conf = config.get('flink', {})
    kafka_conf = config.get('kafka', {})
    
    # 1. Unaligned Checkpoint Coordination
    unaligned = flink_conf.get('unaligned_checkpoints', False)
    if unaligned and flink_conf.get('max_concurrent_checkpoints', 1) > 1:
        reasons.append("C1: Unaligned checkpoints require max_concurrent_checkpoints = 1")
        
    # 2. Checkpoint Interval Bounds [5, 300]s
    interval_ms = flink_conf.get('checkpoint_interval_ms', 60000)
    if not (5000 <= interval_ms <= 300000):
        reasons.append(f"C2: Checkpoint interval {interval_ms}ms outside [5s, 300s] bounds")
        
    # 3. Replication Safety (min_isr <= repl_factor)
    min_isr = kafka_conf.get('min_insync_replicas', 1)
    repl_factor = kafka_conf.get('replication_factor', 1)
    if min_isr > repl_factor:
        reasons.append(f"C3: min_insync_replicas ({min_isr}) > replication_factor ({repl_factor})")
        
    # 4. State Retention Consistency (kafka_ret >= state_ttl)
    retention_ms = kafka_conf.get('retention_ms', 600000)
    state_ttl_ms = flink_conf.get('state_ttl_ms', 300000)
    if retention_ms < state_ttl_ms:
        reasons.append(f"C4: Kafka retention ({retention_ms}ms) < Flink State TTL ({state_ttl_ms}ms)")
        
    # 5. Parallelism-Partition Ratio
    parallelism = flink_conf.get('parallelism', 1)
    partitions = kafka_conf.get('partitions', 4) # Assume 4 for testbed
    if parallelism > partitions:
        reasons.append(f"C5: Parallelism ({parallelism}) > Partition Count ({partitions})")
        
    # 6. Cluster Capacity Checks
    if parallelism > 8: # Limit for Minikube testbed
        reasons.append("C6: Proposed parallelism exceeds cluster slot capacity (8)")
        
    # 7. Memory Allocation Coherence
    managed_mem = flink_conf.get('managed_memory_mb', 256)
    if managed_mem < 128:
        reasons.append("C7: Managed memory too low (< 128MB) for stable state operations")
        
    # 8. State Backend Compatibility
    backend = flink_conf.get('state_backend', 'hashmap')
    incremental = flink_conf.get('incremental_checkpoints', False)
    if incremental and backend != 'rocksdb':
        reasons.append("C8: Incremental checkpoints require RocksDB state backend")
        
    # 9. Configuration Section Consistency
    if 'flink' not in config or 'kafka' not in config:
        reasons.append("C9: Missing required configuration sections (flink/kafka)")
        
    # 10. Change Magnitude Limits (Max 50% change)
    if LAST_VALID_CONFIG:
        old_parallelism = LAST_VALID_CONFIG.get('flink', {}).get('parallelism', parallelism)
        if abs(parallelism - old_parallelism) / old_parallelism > 0.5:
              reasons.append(f"C10: Parallelism change too large (>50% of {old_parallelism})")
              
    # 11. Checkpoint Timeout Ratio (timeout > interval)
    timeout_ms = flink_conf.get('checkpoint_timeout_ms', 120000)
    if timeout_ms <= interval_ms:
        reasons.append(f"C11: Checkpoint timeout ({timeout_ms}ms) must be > interval ({interval_ms}ms)")
        
    if not reasons:
        LAST_VALID_CONFIG = config
        
    return reasons

@app.route('/validate', methods=['POST'])
def validate():
    try:
        config = request.json
        logger.info(f"Validating config: {config}")
        
        reasons = validate_config(config)
        
        if not reasons:
            return jsonify({"valid": True, "reasons": []})
        else:
            return jsonify({"valid": False, "reasons": reasons})

    except Exception as e:
        logger.error(f"Validation error: {e}")
        return jsonify({"valid": False, "reasons": [str(e)]}), 400

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
