# Reproducible Autotuning Framework

This repository contains the reference implementation for the paper "Reproducible Streaming Autotuning Framework".

## Structure

- `services/`: Source code for microservices (Workload Generator, Flink Job, Tuner, Gate).
- `configs/`: Configuration files for scenarios and components.
- `k8s/`: Kubernetes manifests and Helm charts.
- `scripts/`: Utilities for experiment execution and data aggregation.
- `artifacts/`: Generated experimental data (metrics, logs, time-series).

## Experiment Orchestration

### 1. Run Full Experiment Protocol
Executes the robust, sequential N=5 protocol for all scenarios defined in the paper (S1-S4). It handles deployment, metric collection, and artifact capture. This is the primary entry point for reproducing the paper's results.

```bash
python scripts/orchestrate_protocol.py
```

### 2. Run Individual Experiments
For granular control over a single scenario/variant, you can run experiments locally or on a Kubernetes cluster.

#### Local Execution (Docker Compose)
Runs the experiment stack using Docker Compose. This is ideal for development and quick testing.

```bash
python scripts/run_experiment.py S1_steady --variant tuner --repeats 1
```

#### Kubernetes Execution
Executes the experiment on a K8S cluster using Helm. This environment more closely mirrors production deployments.

```bash
# Run the experiment
python scripts/run_k8s_experiment.py S1_steady --variant tuner --repeats 1
```

## Data Aggregation

Generate statistical summaries and recovery metrics from collected artifacts:

```bash
python scripts/aggregate_results.py
```

**Output:** `artifacts/aggregate_report.json` containing:
- Median, IQR, and 95% confidence intervals for all metrics.
- Recovery time analysis for transient failures.
- Statistical significance tests (Wilcoxon) and effect sizes (Cliff's delta).

## License

MIT
