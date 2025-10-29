#!/usr/bin/env python3
"""Kafka Producer - Random log generator"""
import time
import random
import uuid
import json
import argparse
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError


EVENT_TYPES = [
    'zb_screen_view', 'view_item', 'view_item_list', 'zb_click',
    'select_content', 'zb_session_start', 'begin_checkout', 'add_to_cart',
    'first_visit', 'user_engagement', 'page_view', 'scroll',
    'zb_map_view', 'share', 'session_start', 'screen_view',
    'remove_from_cart', 'purchase', 'add_to_wishlist', 'search',
    'view_search_results', 'zb_filter', 'zb_sort', 'sign_up', 'login'
]

SCREEN_NAMES = [
    'item_detail', 'item_list', 'home', 'map_view', 'search_results',
    'my_page', 'favorites', 'checkout', 'cart', 'filter_page'
]

USER_AGENTS = [
    'zigbang/5.1.0 (Android 9; Samsung SM-G960N)',
    'zigbang/5.1.0 (Android 10; Samsung SM-G973N)',
    'zigbang/5.1.0 (Android 11; Samsung SM-G991N)',
    'zigbang/5.0.9 (iOS 14.8; iPhone 11)',
    'zigbang/5.0.9 (iOS 15.0; iPhone 12)',
    'zigbang/5.0.9 (iOS 15.6; iPhone 13)',
    'zigbang/5.1.1 (Android 12; Samsung SM-S908N)',
    'zigbang/5.1.1 (iOS 16.0; iPhone 14)'
]


class LogGenerator:
    def __init__(self):
        self.session_pool = []
        self.max_sessions = 1000

    def generate_log(self):
        now = datetime.now()
        timestamp_ms = int(now.timestamp() * 1000)

        # Reuse session 80% of the time
        if self.session_pool and random.random() < 0.8:
            session_data = random.choice(self.session_pool)
            user_id = session_data['user_id']
            session_id = session_data['session_id']
        else:
            user_id = str(uuid.uuid4())
            session_id = str(uuid.uuid4())
            session_data = {'user_id': user_id, 'session_id': session_id}
            self.session_pool.append(session_data)

            if len(self.session_pool) > self.max_sessions:
                self.session_pool.pop(0)

        # zb_screen_view is most common
        if random.random() < 0.3:
            event_type = 'zb_screen_view'
        else:
            event_type = random.choice(EVENT_TYPES)

        user_agent = random.choice(USER_AGENTS)
        ip_address = f"{random.randint(1, 223)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"
        hash_value = str(hash(f"{user_id}{session_id}{timestamp_ms}"))[-16:]
        timestamp_ms_2 = timestamp_ms + random.randint(0, 100)
        datetime_string = now.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        json_data = self._generate_event_params(event_type)
        json_string = json.dumps(json_data, ensure_ascii=False, separators=(',', ':'))

        log_line = '|'.join([
            str(timestamp_ms),
            user_id,
            session_id,
            event_type,
            user_agent,
            ip_address,
            hash_value,
            str(timestamp_ms_2),
            datetime_string,
            json_string
        ])

        return log_line

    def _generate_event_params(self, event_type):
        params = {}

        if event_type == 'zb_screen_view':
            params['screen_name'] = random.choice(SCREEN_NAMES)
        elif event_type == 'view_item':
            params['item_id'] = f"item_{random.randint(1000, 9999)}"
            params['item_type'] = random.choice(['oneroom', 'tworoom', 'officetel'])
            params['price'] = random.randint(300, 1500) * 10000
        elif event_type == 'zb_click':
            params['element_id'] = f"btn_{random.choice(['detail', 'map', 'filter', 'sort'])}"
        elif event_type == 'search':
            params['search_term'] = random.choice(['gangnam', 'seoul', 'line2', 'yeoksam', 'bundang'])
        elif event_type == 'add_to_cart':
            params['item_id'] = f"item_{random.randint(1000, 9999)}"

        params['timestamp'] = int(datetime.now().timestamp() * 1000)
        return params


def main():
    parser = argparse.ArgumentParser(description='Kafka Log Producer')
    parser.add_argument('--rate', type=int, default=500, help='Logs per second (default: 500)')
    parser.add_argument('--bootstrap-servers', type=str, default='localhost:9092', help='Kafka bootstrap servers')
    parser.add_argument('--topic', type=str, default='zigbang_logs', help='Kafka topic name')
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"Kafka Producer Starting")
    print(f"{'='*60}")
    print(f"  Rate: {args.rate} logs/sec")
    print(f"  Kafka: {args.bootstrap_servers}")
    print(f"  Topic: {args.topic}")
    print(f"{'='*60}\n")

    try:
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_servers,
            value_serializer=lambda v: v.encode('utf-8'),
            acks='all',
            retries=3,
            compression_type='gzip'
        )
        print("✅ Kafka Producer connected\n")
    except KafkaError as e:
        print(f"❌ Kafka connection failed: {e}")
        return

    log_gen = LogGenerator()
    total_sent = 0
    start_time = time.time()
    last_report_time = start_time

    try:
        batch_size = args.rate
        sleep_time = 1.0 / batch_size if batch_size > 0 else 0.001

        print("🚀 Sending logs... (Ctrl+C to stop)\n")

        while True:
            batch_start = time.time()

            for _ in range(batch_size):
                log = log_gen.generate_log()
                future = producer.send(args.topic, value=log)
                total_sent += 1

                if sleep_time > 0:
                    time.sleep(sleep_time)

            current_time = time.time()
            if current_time - last_report_time >= 10.0:
                elapsed = current_time - start_time
                rate = total_sent / elapsed
                print(f"📊 Sent: {total_sent:,} | Avg rate: {rate:.1f} logs/sec | Sessions: {len(log_gen.session_pool)}")
                last_report_time = current_time

            batch_elapsed = time.time() - batch_start
            if batch_elapsed < 1.0:
                time.sleep(1.0 - batch_elapsed)

    except KeyboardInterrupt:
        print(f"\n\n{'='*60}")
        print("⏹  Stop requested")
        print(f"{'='*60}")
    finally:
        producer.flush()
        producer.close()

        elapsed = time.time() - start_time
        avg_rate = total_sent / elapsed if elapsed > 0 else 0

        print(f"\n📊 Final stats:")
        print(f"   Total sent: {total_sent:,}")
        print(f"   Runtime: {elapsed:.1f}s")
        print(f"   Avg rate: {avg_rate:.1f} logs/sec")
        print(f"\n✅ Producer stopped\n")


if __name__ == "__main__":
    main()
