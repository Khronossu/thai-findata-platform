"""
Unit tests for producers/promptpay_producer.py

Layer 1 — pure Python, no Kafka, no Docker required.
All tests call the generator functions directly and assert
on the output dicts. Nothing is sent to a broker.
"""
import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from producers.promptpay_producer import (
    REGIONS,
    CHANNELS,
    MERCHANT_CATEGORIES,
    DISTANT_REGION_PAIRS,
    SENDER_POOL,
    pick_region,
    pick_coords,
    pick_channel,
    generate_normal_event,
    generate_velocity_burst,
    generate_amount_spike,
    generate_impossible_geography,
    generate_dormant_spike,
    generate_anomaly_events,
)

# ── FIXTURES ────────────────────────────────────────────────────

@pytest.fixture
def sender():
    return SENDER_POOL[0]

# ── REGION / COORD HELPERS ──────────────────────────────────────

def test_pick_region_returns_valid_region():
    for _ in range(50):
        assert pick_region() in REGIONS

def test_pick_coords_within_thailand_bounds():
    """Thailand rough bounding box: lat 5–21, lng 97–106."""
    for region in REGIONS:
        for _ in range(10):
            lat, lng = pick_coords(region)
            assert 5.0 <= lat <= 21.0, f"lat {lat} out of bounds for {region}"
            assert 97.0 <= lng <= 106.0, f"lng {lng} out of bounds for {region}"

def test_region_weights_sum_to_one():
    total = sum(r['weight'] for r in REGIONS.values())
    assert abs(total - 1.0) < 1e-9

# ── CHANNEL HELPER ──────────────────────────────────────────────

def test_pick_channel_returns_valid_channel():
    for _ in range(50):
        assert pick_channel() in CHANNELS

def test_channel_weights_sum_to_one():
    total = sum(CHANNELS.values())
    assert abs(total - 1.0) < 1e-9

def test_channel_no_duplicates():
    """Channels must use weighted dict, not duplicate-list trick."""
    assert len(CHANNELS) == len(set(CHANNELS.keys()))

# ── NORMAL EVENT ────────────────────────────────────────────────

REQUIRED_FIELDS = {
    'transaction_id', 'sender_national_id', 'sender_account',
    'receiver_account', 'amount_thb', 'merchant_category', 'channel',
    'location_lat', 'location_lng', 'event_timestamp',
    'is_anomaly', 'anomaly_type',
}

def test_normal_event_has_all_fields(sender):
    event = generate_normal_event(sender)
    assert REQUIRED_FIELDS.issubset(event.keys())

def test_normal_event_amount_positive(sender):
    for _ in range(20):
        event = generate_normal_event(sender)
        assert event['amount_thb'] >= 1.0

def test_normal_event_not_anomaly(sender):
    event = generate_normal_event(sender)
    assert event['is_anomaly'] is False
    assert event['anomaly_type'] is None

def test_normal_event_merchant_category_valid(sender):
    for _ in range(20):
        event = generate_normal_event(sender)
        assert event['merchant_category'] in MERCHANT_CATEGORIES

def test_normal_event_channel_valid(sender):
    for _ in range(20):
        event = generate_normal_event(sender)
        assert event['channel'] in CHANNELS

def test_normal_event_transaction_id_unique(sender):
    ids = [generate_normal_event(sender)['transaction_id'] for _ in range(100)]
    assert len(set(ids)) == 100

def test_normal_event_coords_in_thailand(sender):
    for _ in range(20):
        event = generate_normal_event(sender)
        assert 5.0 <= event['location_lat'] <= 21.0
        assert 97.0 <= event['location_lng'] <= 106.0

# ── VELOCITY BURST ──────────────────────────────────────────────

def test_velocity_burst_count(sender):
    for _ in range(10):
        events = generate_velocity_burst(sender)
        assert 8 <= len(events) <= 12

def test_velocity_burst_all_flagged(sender):
    events = generate_velocity_burst(sender)
    for e in events:
        assert e['is_anomaly'] is True
        assert e['anomaly_type'] == 'velocity_burst'

def test_velocity_burst_same_sender(sender):
    events = generate_velocity_burst(sender)
    for e in events:
        assert e['sender_national_id'] == sender['national_id']

def test_velocity_burst_different_receivers(sender):
    events = generate_velocity_burst(sender)
    receivers = {e['receiver_account'] for e in events}
    # With 8–12 events, expect most receivers to be unique
    assert len(receivers) > 1

# ── AMOUNT SPIKE ────────────────────────────────────────────────

def test_amount_spike_flagged(sender):
    event = generate_amount_spike(sender)
    assert event['is_anomaly'] is True
    assert event['anomaly_type'] == 'amount_spike'

def test_amount_spike_magnitude(sender):
    """Amount must be at least 40x the sender's typical average."""
    for _ in range(10):
        event = generate_amount_spike(sender)
        assert event['amount_thb'] >= sender['typical_amount_avg'] * 40

# ── IMPOSSIBLE GEOGRAPHY ────────────────────────────────────────

def test_impossible_geography_returns_two_events(sender):
    for _ in range(10):
        events = generate_impossible_geography(sender)
        assert len(events) == 2

def test_impossible_geography_both_flagged(sender):
    events = generate_impossible_geography(sender)
    for e in events:
        assert e['is_anomaly'] is True
        assert e['anomaly_type'] == 'impossible_geography'

def test_impossible_geography_different_locations(sender):
    """The two events must be in genuinely different regions."""
    for _ in range(10):
        events = generate_impossible_geography(sender)
        lat_diff = abs(events[0]['location_lat'] - events[1]['location_lat'])
        lng_diff = abs(events[0]['location_lng'] - events[1]['location_lng'])
        # Minimum separation from the closest distant pair (bangkok/northeast ~400km)
        assert lat_diff > 0.5 or lng_diff > 0.5

def test_impossible_geography_uses_valid_region_pairs():
    valid_regions = set(REGIONS.keys())
    for region_a, region_b in DISTANT_REGION_PAIRS:
        assert region_a in valid_regions
        assert region_b in valid_regions
        assert region_a != region_b

# ── DORMANT SPIKE ───────────────────────────────────────────────

def test_dormant_spike_count(sender):
    for _ in range(5):
        events = generate_dormant_spike(sender)
        assert 15 <= len(events) <= 25

def test_dormant_spike_all_flagged(sender):
    events = generate_dormant_spike(sender)
    for e in events:
        assert e['is_anomaly'] is True
        assert e['anomaly_type'] == 'dormant_spike'

def test_dormant_spike_amount_range(sender):
    events = generate_dormant_spike(sender)
    for e in events:
        assert 800 <= e['amount_thb'] <= 3000

# ── ANOMALY DISPATCHER ──────────────────────────────────────────

VALID_ANOMALY_TYPES = {'velocity_burst', 'amount_spike', 'impossible_geography', 'dormant_spike'}

def test_anomaly_dispatcher_always_returns_events(sender):
    for _ in range(50):
        events = generate_anomaly_events(sender)
        assert len(events) >= 1

def test_anomaly_dispatcher_valid_types(sender):
    for _ in range(50):
        events = generate_anomaly_events(sender)
        for e in events:
            assert e['anomaly_type'] in VALID_ANOMALY_TYPES

# ── SENDER POOL ─────────────────────────────────────────────────

def test_sender_pool_size():
    assert len(SENDER_POOL) == 500

def test_sender_pool_valid_home_regions():
    for sender in SENDER_POOL:
        assert sender['home_region'] in REGIONS

def test_sender_pool_national_id_length():
    for sender in SENDER_POOL:
        assert len(sender['national_id']) == 13
        assert sender['national_id'].isdigit()
