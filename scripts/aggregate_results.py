import os
import json
import numpy as np
import pandas as pd
from scipy import stats
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Aggregator")

ROOT_DIR = Path(__file__).parent.parent
ARTIFACTS_DIR = ROOT_DIR / "artifacts"

def bootstrap_ci(data, n_resamples=2000, confidence_level=0.95):
    if len(data) < 2:
        return (np.nan, np.nan)
    res = stats.bootstrap((data,), np.median, confidence_level=confidence_level, n_resamples=n_resamples, method='percentile')
    return (res.confidence_interval.low, res.confidence_interval.high)

def calculate_cliffs_delta(x, y):
    n_x = len(x)
    n_y = len(y)
    count = 0
    for i in x:
        for j in y:
            if i > j:
                count += 1
            elif i < j:
                count -= 1
    return count / (n_x * n_y)

def aggregate_results():
    results = []
    
    # 1. Collect all result files
    for result_path in ARTIFACTS_DIR.glob("**/metrics.json"):
        try:
            with open(result_path, "r") as f:
                data = json.load(f)
                
            meta = data.get("run_meta", {})
            summary = data.get("summary", {})
            
            # Extract scenario and variant from run_id or folder name
            # Format: S1_steady_baseline_rep1
            run_id = meta.get("run_id", result_path.parent.name)
            parts = run_id.split("_")
            if len(parts) >= 3:
                scenario = "_".join(parts[:-2])
                variant = parts[-2]
                rep = parts[-1].replace("rep", "")
            else:
                scenario = meta.get("scenario_id", "unknown")
                variant = "unknown"
                rep = "unknown"

            flat_res = {
                "scenario": scenario,
                "variant": variant,
                "rep": rep,
                "latency_p95": summary.get("e2e_latency_ms", {}).get("p95", 0),
                "latency_p99": summary.get("e2e_latency_ms", {}).get("p99", 0),
                "throughput_out": summary.get("throughput_out_eps", {}).get("mean", 0),
                "consumer_lag": summary.get("consumer_lag_records", {}).get("max", 0),
                "backpressure": summary.get("backpressure_ratio", {}).get("mean", 0),
                "cpu_usage": summary.get("cpu_usage_cores", {}).get("p95_total", 0)
            }
            results.append(flat_res)
        except Exception as e:
            logger.error(f"Failed to parse {result_path}: {e}")

    if not results:
        logger.warning("No results found to aggregate.")
        return

    df = pd.DataFrame(results)
    
    # 2. Group by scenario and variant
    agg_report = {}
    
    for scenario in df['scenario'].unique():
        scenario_df = df[df['scenario'] == scenario]
        agg_report[scenario] = {}
        
        variants = scenario_df['variant'].unique()
        for variant in variants:
            v_df = scenario_df[scenario_df['variant'] == variant]
            
            stats_v = {}
            for metric in ["latency_p95", "latency_p99", "throughput_out", "consumer_lag", "backpressure", "cpu_usage"]:
                data = v_df[metric].values
                median = np.median(data)
                iqr = stats.iqr(data)
                ci_low, ci_high = bootstrap_ci(data)
                
                stats_v[metric] = {
                    "median": float(median),
                    "iqr": float(iqr),
                    "ci_95": [float(ci_low), float(ci_high)],
                    "samples": [float(x) for x in data]
                }
            
            agg_report[scenario][variant] = stats_v

            # Calculate Recovery Time for S2 and S3
            if scenario in ["S2_spike", "S3_failure"]:
                recovery_times = []
                for rep_id in v_df['rep']:
                     # Construct path to timeseries
                     ts_path = ARTIFACTS_DIR / f"{scenario}_{variant}_rep{rep_id}" / "metrics_timeseries.json"
                     if ts_path.exists():
                         try:
                             with open(ts_path) as f:
                                 ts_data = json.load(f)
                             
                             # Find latency metric
                             lat_series = next((m for m in ts_data["metrics"] if m["name"] == "e2e_latency_ms"), None)
                             if lat_series:
                                 # Find start of spike/failure
                                 # We know spike starts at T=600.
                                 # Find time it takes to return to < 3000ms (SLO)
                                 
                                 start_time = ts_data["time_range"]["start_time"]
                                 spike_start_offset = 600
                                 
                                 values = lat_series["values"]
                                 
                                 breach_start = None
                                 recovery_end = None
                                 
                                 for t, v_str in values:
                                     offset = t - start_time
                                     val = float(v_str)
                                     
                                     if offset >= spike_start_offset:
                                         if val > 3000:
                                             if breach_start is None:
                                                 breach_start = offset
                                         elif breach_start is not None and val < 3000:
                                             # Check stability: next 10 samples must be < 3000
                                             # Find current index from t
                                             current_idx = int(offset) # roughly, since interval is 1s
                                             # But values is a list of [t, v].
                                             # Let's verify consecutive values in the list.
                                             # This loop iterates values. We can peek ahead?
                                             # Iterator doesn't allow peek.
                                             # Using index based loop is better, but values is list of list.
                                             # Let's switch to checking "stable_count"
                                             pass

                                 # Re-implement with index loop
                                 breach_start = None
                                 recovery_end = None
                                 
                                 N = len(values)
                                 idx_spike = next((i for i, v in enumerate(values) if v[0] - start_time >= spike_start_offset), 0)
                                 
                                 for i in range(idx_spike, N):
                                     t = values[i][0] - start_time
                                     val = float(values[i][1])
                                     
                                     if val > 3000:
                                         if breach_start is None:
                                             breach_start = t
                                     elif breach_start is not None and val < 3000:
                                         # Check next 10
                                         is_stable = True
                                         for j in range(1, 11):
                                             if i + j < N:
                                                 if float(values[i+j][1]) >= 3000:
                                                     is_stable = False
                                                     break
                                         
                                         if is_stable:
                                             recovery_end = t
                                             break
                                 
                                 if breach_start and recovery_end:
                                     rec_time_min = (recovery_end - breach_start) / 60.0
                                     recovery_times.append(rec_time_min)
                                 elif breach_start and not recovery_end:
                                     # Never recovered
                                     recovery_times.append(20.0) # Cap at run end
                                 else:
                                     # No breach
                                     recovery_times.append(0.0)
                         except Exception as e:
                             logger.warning(f"Failed to calc recovery for {ts_path}: {e}")
                
                if recovery_times:
                    agg_report[scenario][variant]["recovery_time_min"] = {
                        "median": float(np.median(recovery_times)),
                        "mean": float(np.mean(recovery_times)),
                        "samples": recovery_times
                    }

        # 3. Statistical Comparison (Baseline vs Tuner)
        if "baseline" in variants and "tuner" in variants:
            baseline_df = scenario_df[scenario_df['variant'] == "baseline"].sort_values("rep")
            tuner_df = scenario_df[scenario_df['variant'] == "tuner"].sort_values("rep")
            
            # Ensure paired reps
            common_reps = set(baseline_df['rep']).intersection(set(tuner_df['rep']))
            b_paired = baseline_df[baseline_df['rep'].isin(common_reps)]['latency_p95'].values
            t_paired = tuner_df[tuner_df['rep'].isin(common_reps)]['latency_p95'].values
            
            if len(b_paired) >= 5:
                # Wilcoxon signed-rank test
                stat, p_val = stats.wilcoxon(b_paired, t_paired)
                delta = calculate_cliffs_delta(t_paired, b_paired)
                
                agg_report[scenario]["comparison"] = {
                    "metric": "latency_p95",
                    "test": "wilcoxon",
                    "p_value": float(p_val),
                    "cliffs_delta": float(delta),
                    "interpretation": "Significant improvement" if p_val < 0.05 and delta < 0 else "No significant difference"
                }

    # 4. Save Aggregated Report
    output_path = ARTIFACTS_DIR / "aggregate_report.json"
    with open(output_path, "w") as f:
        json.dump(agg_report, f, indent=2)
    
    logger.info(f"Aggregated report saved to {output_path}")

if __name__ == "__main__":
    aggregate_results()
