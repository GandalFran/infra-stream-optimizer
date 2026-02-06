import os
import sys
import time
import json
import logging
import argparse
import signal
from kafka import KafkaProducer
from threading import Event

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

stop_event = Event()

def signal_handler(sig, frame):
    logger.info("Stopping workload generator...")
    stop_event.set()

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def get_producer(bootstrap_servers):
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda v: str(v).encode('utf-8') if v else None
            )
            return producer
        except Exception as e:
            logger.warning(f"Failed to connect to Kafka: {e}. Retrying in 5s...")
            time.sleep(5)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="Path to scenario config (optional)")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "trips_in"))
    parser.add_argument("--rate", type=float, default=float(os.getenv("TARGET_RATE", "10.0")), help="Events per second")
    parser.add_argument("--file", default=os.getenv("DATA_FILE", "dummy"), help="Path to JSONL file")
    args = parser.parse_args()

    producer = get_producer(args.bootstrap_servers)
    logger.info(f"Connected to Kafka at {args.bootstrap_servers}, targeting topic {args.topic}")

    count = 0
    start_time = time.time()
    
    logger.info(f"Starting generation at {args.rate} events/sec from {args.file}")

    try:
        if args.file == "dummy":
            logger.warning("No file specified, running in dummy mode.")
            # Dummy mode (legacy)
            while not stop_event.is_set():
                loop_start = time.time()
                event = {
                    "event_id": f"evt-{count}",
                    "event_time_ms": int(time.time() * 1000),
                    "sent_time_ms": int(time.time() * 1000),  # Required for Flink watermark
                    "payload": "dummy-data" * 5
                }
                producer.send(args.topic, value=event, key=str(count % 10))
                if count % 1000 == 0:
                    logger.info(f"Sample Event: {json.dumps(event)}")
                count += 1
                
                elapsed = time.time() - start_time
                expected_count = elapsed * args.rate
                if count > expected_count:
                    sleep_time = (count - expected_count) / args.rate
                    if sleep_time > 0:
                        time.sleep(sleep_time)
        else:
            # File Replay Mode
            with open(args.file, 'r') as f:
                # Load Rate Schedule if config present
                rate_schedule = None
                base_rate = args.rate
                if args.config:
                    try:
                        import yaml
                        with open(args.config, 'r') as yf:
                            cfg = yaml.safe_load(yf)
                            rc = cfg.get('rate_control', {})
                            if rc.get('mode') == 'rate_driven':
                                base_rate = float(rc.get('target_rate_events_per_sec', base_rate))
                                if 'step_at_sec' in rc:
                                    rate_schedule = {
                                        'step_at': rc['step_at_sec'],
                                        'step_rate': rc['step_rate_events_per_sec']
                                    }
                                    logger.info(f"Rate schedule loaded: {rate_schedule}")
                    except ImportError:
                        logger.warning("PyYAML not found, using static rate")
                    except Exception as e:
                        logger.warning(f"Failed to load config rate: {e}")

                current_rate = base_rate
                logger.info(f"Starting generation at {current_rate} events/sec from {args.file}")
                
                for line in f:
                    if stop_event.is_set():
                        break
                        
                    event = json.loads(line)
                    
                    # R1 Rate Driven: Update timestamp to now, but keep original event properties
                    event['sent_time_ms'] = int(time.time() * 1000)
                    
                    # Check for payload padding (S4 Drift)
                    if 'perturbations' in cfg and 'payload_padding' in cfg['perturbations']:
                         pp = cfg['perturbations']['payload_padding']
                         if pp.get('enabled') and time.time() - start_time >= pp.get('start_at_sec', 0):
                              pad_size = pp.get('pad_bytes', 0)
                              if pad_size > 0:
                                   event['payload'] = event.get('payload', "") + "X" * pad_size
                    
                    producer.send(args.topic, value=event, key=event.get('key'))
                    count += 1
                    
                    elapsed = time.time() - start_time
                    
                    # Check for rate step
                    if rate_schedule and elapsed >= rate_schedule['step_at']:
                        if current_rate != rate_schedule['step_rate']:
                            logger.info(f"Stepping rate to {rate_schedule['step_rate']} at t={elapsed:.1f}s")
                            current_rate = rate_schedule['step_rate']
                    
                    expected_count = 0
                    # Simple integral approximation for expected count
                    if rate_schedule and elapsed >= rate_schedule['step_at']:
                        # Events before step
                        expected_count += base_rate * rate_schedule['step_at']
                        # Events after step
                        expected_count += current_rate * (elapsed - rate_schedule['step_at'])
                    else:
                        expected_count = elapsed * current_rate

                    if count > expected_count:
                        sleep_time = (count - expected_count) / current_rate
                        if sleep_time > 0:
                            time.sleep(sleep_time)
                    
                    if count % 2000 == 0:
                        logger.info(f"Sent {count} events. Current Rate: {current_rate} eps. Avg: {count/elapsed:.2f} eps")
            
            logger.info("Finished reading file.")

    except Exception as e:
        logger.error(f"Error in generator loop: {e}")
    finally:
        producer.close()
        logger.info("Generator stopped.")

if __name__ == "__main__":
    main()
