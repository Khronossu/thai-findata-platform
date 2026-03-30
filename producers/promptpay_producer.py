import json
import time
import random
import uuid
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer
import os

# ── CONFIG ─────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.environ['KAFKA_BOOTSTRAP_SERVERS']
TOPIC = 'promptpay_transactions'
TARGET_RATE = 10000          # events per minute
BATCH_SIZE = 100             # events per batch
BATCH_INTERVAL = 0.6         # seconds between batches (100 events * 100 batches = 10k/min)
ANOMALY_RATE = 0.05          # 5% of events are anomalous

# ── REFERENCE DATA ─────────────────────────────────────────────
MERCHANT_CATEGORIES = {
    'retail':       {'weight': 0.35, 'avg_amount': 850,   'std': 600},
    'food':         {'weight': 0.25, 'avg_amount': 320,   'std': 180},
    'transport':    {'weight': 0.15, 'avg_amount': 150,   'std': 80},
    'utility':      {'weight': 0.10, 'avg_amount': 1200,  'std': 400},
    'healthcare':   {'weight': 0.08, 'avg_amount': 2500,  'std': 1500},
    'education':    {'weight': 0.07, 'avg_amount': 8000,  'std': 3000},
}

CHANNELS = {
    'mobile_app': 0.65,
    'web':        0.22,
    'atm':        0.13,
}

# ── HELPERS ────────────────────────────────────────────────────

def pick_channel() -> str:
    return random.choices(
        list(CHANNELS.keys()),
        weights=list(CHANNELS.values()),
    )[0]

# ── SENDER POOL ────────────────────────────────────────────────
# Pre-generate a fixed pool of senders so velocity/behavioral
# anomalies reference real recurring senders, not random IDs
def generate_sender_pool(size: int = 500) -> list[dict]:
    pool = []
    for _ in range(size):
        national_id = ''.join([str(random.randint(0, 9)) for _ in range(13)])
        pool.append({
            'national_id': national_id,
            'account': f'TH{random.randint(10**9, 10**10 - 1)}',
            'typical_amount_avg': random.uniform(300, 3000),
            'transaction_count_today': 0,
        })
    return pool

SENDER_POOL = generate_sender_pool(500)

# ── NORMAL EVENT ───────────────────────────────────────────────
def generate_normal_event(sender: dict) -> dict:
    category = random.choices(
        list(MERCHANT_CATEGORIES.keys()),
        weights=[v['weight'] for v in MERCHANT_CATEGORIES.values()]
    )[0]
    
    cat = MERCHANT_CATEGORIES[category]
    amount = max(1.0, random.gauss(cat['avg_amount'], cat['std']))
    amount = round(amount, 2)
    

    return {
        'transaction_id': str(uuid.uuid4()),
        'sender_national_id': sender['national_id'],
        'sender_account': sender['account'],
        'receiver_account': f'TH{random.randint(10**9, 10**10 - 1)}',
        'amount_thb': amount,
        'merchant_category': category,
        'channel': pick_channel(),
        'event_timestamp': datetime.now(timezone.utc).isoformat(),
        'is_anomaly': False,
        'anomaly_type': None,
    }

# ── ANOMALY PATTERNS ───────────────────────────────────────────
def generate_velocity_burst(sender: dict) -> list[dict]:
    """
    Same sender, 8-12 transactions, different receivers, 
    within a 90-second window. Each event is valid individually.
    Fraud signal: frequency.
    """
    events = []
    burst_count = random.randint(8, 12)
    base_time = datetime.now(timezone.utc)
    
    for i in range(burst_count):
        event = generate_normal_event(sender)
        # Spread timestamps within 90 seconds
        offset_seconds = random.uniform(0, 90)

        ts = base_time + timedelta(seconds=offset_seconds)
        event['event_timestamp'] = ts.isoformat()
        event['is_anomaly'] = True
        event['anomaly_type'] = 'velocity_burst'
        events.append(event)
    
    return events

def generate_amount_spike(sender: dict) -> dict:
    """
    Amount 40-80x the sender's typical average.
    Normal category, normal receiver, abnormal amount.
    Fraud signal: statistical deviation from sender baseline.
    """
    event = generate_normal_event(sender)
    multiplier = random.uniform(40, 80)
    event['amount_thb'] = round(sender['typical_amount_avg'] * multiplier, 2)
    event['is_anomaly'] = True
    event['anomaly_type'] = 'amount_spike'
    return event

def generate_dormant_spike(sender: dict) -> list[dict]:
    """
    Sender has low transaction_count_today (simulated as 0-2).
    Suddenly fires 15-25 transactions in one batch.
    Fraud signal: behavioral change from account baseline.
    """
    events = []
    spike_count = random.randint(15, 25)
    
    for _ in range(spike_count):
        event = generate_normal_event(sender)
        # Dormant accounts typically send smaller amounts
        # Spike uses slightly higher amounts to make it detectable
        event['amount_thb'] = round(random.uniform(800, 3000), 2)
        event['is_anomaly'] = True
        event['anomaly_type'] = 'dormant_spike'
        events.append(event)
    
    return events

# ── ANOMALY DISPATCHER ─────────────────────────────────────────
ANOMALY_PATTERNS = [
    (0.50, 'velocity',   lambda s: generate_velocity_burst(s)),
    (0.35, 'amount',     lambda s: [generate_amount_spike(s)]),
    (0.15, 'dormant',    lambda s: generate_dormant_spike(s)),
]

def generate_anomaly_events(sender: dict) -> list[dict]:
    roll = random.random()
    cumulative = 0
    for weight, _, generator in ANOMALY_PATTERNS:
        cumulative += weight
        if roll <= cumulative:
            return generator(sender)
    return [generate_amount_spike(sender)]  # fallback

# ── PRODUCER ───────────────────────────────────────────────────
def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8'),
        # Batch settings for throughput
        batch_size=16384,
        linger_ms=10,
        compression_type='gzip',
        acks='all',  # wait for broker acknowledgment — no silent data loss
    )

def run():
    producer = create_producer()
    total_sent = 0
    anomaly_count = 0
    
    print(f"Starting PromptPay producer — target {TARGET_RATE} events/min")
    print(f"Anomaly rate: {ANOMALY_RATE*100}% | Patterns: velocity, amount_spike, dormant_spike")
    
    try:
        while True:
            batch_events = []
            
            for _ in range(BATCH_SIZE):
                sender = random.choice(SENDER_POOL)
                
                if random.random() < ANOMALY_RATE:
                    events = generate_anomaly_events(sender)
                    anomaly_count += len(events)
                else:
                    events = [generate_normal_event(sender)]
                
                batch_events.extend(events)
            
            for event in batch_events:
                producer.send(
                    TOPIC,
                    key=event['sender_national_id'],  # partition by sender for ordering
                    value=event,
                )
            
            producer.flush()
            total_sent += len(batch_events)
            
            print(
                f"Sent: {total_sent:,} | "
                f"Anomalies: {anomaly_count:,} ({anomaly_count/max(total_sent,1)*100:.1f}%) | "
                f"Rate: ~{int(BATCH_SIZE/BATCH_INTERVAL*60):,}/min"
            )
            
            time.sleep(BATCH_INTERVAL)
    
    except KeyboardInterrupt:
        print(f"\nStopped. Total sent: {total_sent:,} | Anomalies: {anomaly_count:,}")
        producer.close()

if __name__ == '__main__':
    run()