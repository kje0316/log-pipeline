#!/usr/bin/env python3
"""Kafka to S3 Consumer - Batch log collector"""
import time
import argparse
import gzip
import os
import sys
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

from src.config.config import Config


class BatchLogConsumer:
    def __init__(self, bootstrap_servers, topic, batch_size=1000, flush_interval=60):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            auto_commit_interval_ms=5000,
            group_id='log-s3-consumer-group',
            value_deserializer=lambda m: m.decode('utf-8')
        )

        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.buffer = []
        self.last_flush_time = time.time()
        self.total_consumed = 0
        self.total_saved = 0

    def save_batch_to_file(self, logs):
        if not logs:
            return

        current_date = datetime.now().strftime('%Y-%m-%d')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Save path: data/date=YYYY-MM-DD/streaming_{timestamp}.csv.gz
        data_path = Config.get_data_path()
        partition_path = f"{data_path}date={current_date}"

        os.makedirs(partition_path, exist_ok=True)

        filename = f"streaming_{timestamp}.csv.gz"
        filepath = os.path.join(partition_path, filename)

        try:
            with gzip.open(filepath, 'wt', encoding='utf-8') as f:
                for log in logs:
                    f.write(log + '\n')

            self.total_saved += len(logs)
            print(f"💾 Saved: {filename} ({len(logs):,} logs) -> {partition_path}")

        except Exception as e:
            print(f"❌ Save failed: {e}")

    def should_flush(self):
        buffer_full = len(self.buffer) >= self.batch_size
        time_exceeded = (time.time() - self.last_flush_time) >= self.flush_interval
        return buffer_full or time_exceeded

    def flush_buffer(self):
        if self.buffer:
            self.save_batch_to_file(self.buffer)
            self.buffer = []
            self.last_flush_time = time.time()

    def consume(self):
        print(f"\n{'='*60}")
        print(f"Kafka Consumer Starting")
        print(f"{'='*60}")
        print(f"  Batch size: {self.batch_size}")
        print(f"  Flush interval: {self.flush_interval}s")
        print(f"  Save path: {Config.get_data_path()}")
        print(f"{'='*60}\n")
        print("🚀 Receiving logs... (Ctrl+C to stop)\n")

        last_report_time = time.time()

        try:
            for message in self.consumer:
                log = message.value
                self.buffer.append(log)
                self.total_consumed += 1

                if self.should_flush():
                    self.flush_buffer()

                current_time = time.time()
                if current_time - last_report_time >= 10.0:
                    print(f"📊 Received: {self.total_consumed:,} | Saved: {self.total_saved:,} | Buffer: {len(self.buffer)}")
                    last_report_time = current_time

        except KeyboardInterrupt:
            print(f"\n\n{'='*60}")
            print("⏹  Stop requested")
            print(f"{'='*60}")
        finally:
            print("\n🔄 Flushing remaining data...")
            self.flush_buffer()
            self.consumer.close()

            print(f"\n📊 Final stats:")
            print(f"   Total received: {self.total_consumed:,}")
            print(f"   Total saved: {self.total_saved:,}")
            print(f"\n✅ Consumer stopped\n")


def main():
    parser = argparse.ArgumentParser(description='Kafka to S3 Consumer')
    parser.add_argument('--bootstrap-servers', type=str, default='localhost:9092', help='Kafka bootstrap servers')
    parser.add_argument('--topic', type=str, default='zigbang_logs', help='Kafka topic name')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size (default: 1000)')
    parser.add_argument('--flush-interval', type=int, default=60, help='Flush interval in seconds (default: 60)')
    args = parser.parse_args()

    consumer = BatchLogConsumer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        batch_size=args.batch_size,
        flush_interval=args.flush_interval
    )

    consumer.consume()


if __name__ == "__main__":
    main()
