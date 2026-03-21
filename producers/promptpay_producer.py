import json
import time
import random
import uuid
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer

# ── CONFIG ─────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = 'localhost:9092'
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

REGIONS = {
    'bangkok': {
        'weight': 0.55,
        'coords': [
            (13.7563, 100.5018),   # Rattanakosin
            (13.7308, 100.5204),   # Silom
            (13.7480, 100.5347),   # Asok
            (13.7956, 100.5508),   # Chatuchak
            (13.6900, 100.5993),   # Bangna
            (13.7100, 100.4957),   # Thonburi
            (13.8136, 100.5614),   # Don Mueang
            (13.6757, 100.6081),   # Samut Prakan fringe
        ],
        'jitter_std': 0.015,
    },
    'perimeter': {
        'weight': 0.15,
        'coords': [
            (13.9920, 100.6175),   # Pathum Thani
            (13.5383, 100.4738),   # Samut Sakhon
            (14.0723, 100.6068),   # Rangsit
            (13.6531, 100.6466),   # Samut Prakan
        ],
        'jitter_std': 0.03,
    },
    'central': {
        'weight': 0.08,
        'coords': [
            (14.3514, 100.5770),   # Ayutthaya
            (14.9743, 100.4025),   # Nakhon Sawan
            (13.3622, 100.9847),   # Chonburi / EEC
        ],
        'jitter_std': 0.04,
    },
    'north': {
        'weight': 0.07,
        'coords': [
            (18.7883, 98.9853),    # Chiang Mai — Nimman
            (18.7961, 99.0003),    # Chiang Mai — Old City
            (17.0065, 99.8318),    # Phitsanulok
        ],
        'jitter_std': 0.04,
    },
    'northeast': {
        'weight': 0.07,
        'coords': [
            (14.9798, 102.0978),   # Nakhon Ratchasima
            (16.4322, 102.8236),   # Khon Kaen
            (15.2287, 104.8571),   # Ubon Ratchathani
        ],
        'jitter_std': 0.04,
    },
    'south': {
        'weight': 0.08,
        'coords': [
            (7.8804, 98.3923),     # Phuket
            (9.1382, 99.3211),     # Surat Thani
            (7.0086, 100.4747),    # Hat Yai
            (8.0589, 98.9183),     # Krabi
        ],
        'jitter_std': 0.04,
    },
}

CHANNELS = {
    'mobile_app': 0.60,
    'web':        0.22,
    'atm':        0.13,
    'counter':    0.05,
}

# ── HELPERS ────────────────────────────────────────────────────
def pick_region() -> str:
    return random.choices(
        list(REGIONS.keys()),
        weights=[r['weight'] for r in REGIONS.values()],
    )[0]

def pick_coords(region: str) -> tuple[float, float]:
    r = REGIONS[region]
    base = random.choice(r['coords'])
    return (
        base[0] + random.gauss(0, r['jitter_std']),
        base[1] + random.gauss(0, r['jitter_std']),
    )

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
        region = pick_region()
        pool.append({
            'national_id': national_id,
            'account': f'TH{random.randint(10**9, 10**10 - 1)}',
            'home_region': region,
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
    
    lat, lng = pick_coords(sender['home_region'])

    return {
        'transaction_id': str(uuid.uuid4()),
        'sender_national_id': sender['national_id'],
        'sender_account': sender['account'],
        'receiver_account': f'TH{random.randint(10**9, 10**10 - 1)}',
        'amount_thb': amount,
        'merchant_category': category,
        'channel': pick_channel(),
        'location_lat': round(lat, 6),
        'location_lng': round(lng, 6),
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

DISTANT_REGION_PAIRS = [
    ('bangkok', 'north'),       # ~700 km
    ('bangkok', 'south'),       # ~800 km
    ('bangkok', 'northeast'),   # ~400 km
    ('north',   'south'),       # ~1500 km
    ('northeast', 'south'),     # ~1000 km
]

def generate_impossible_geography(sender: dict) -> list[dict]:
    """
    Two transactions from the same sender, in two distant regions,
    minutes apart — physically impossible travel time.
    """
    base_time = datetime.now(timezone.utc)

    region_a, region_b = random.choice(DISTANT_REGION_PAIRS)
    gap_minutes = random.randint(3, 12)

    event_a = generate_normal_event(sender)
    lat_a, lng_a = pick_coords(region_a)
    event_a['location_lat'] = round(lat_a, 6)
    event_a['location_lng'] = round(lng_a, 6)
    event_a['event_timestamp'] = base_time.isoformat()
    event_a['is_anomaly'] = True
    event_a['anomaly_type'] = 'impossible_geography'

    event_b = generate_normal_event(sender)
    lat_b, lng_b = pick_coords(region_b)
    event_b['location_lat'] = round(lat_b, 6)
    event_b['location_lng'] = round(lng_b, 6)
    event_b['event_timestamp'] = (base_time + timedelta(minutes=gap_minutes)).isoformat()
    event_b['is_anomaly'] = True
    event_b['anomaly_type'] = 'impossible_geography'

    return [event_a, event_b]

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
    (0.40, 'velocity',   lambda s: generate_velocity_burst(s)),
    (0.25, 'amount',     lambda s: [generate_amount_spike(s)]),
    (0.20, 'geography',  lambda s: generate_impossible_geography(s)),
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
    print(f"Anomaly rate: {ANOMALY_RATE*100}% | Patterns: velocity, amount_spike, impossible_geography, dormant_spike")
    
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