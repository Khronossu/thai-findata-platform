import yfinance as yf
import json
import os
import random
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = os.environ['KAFKA_BOOTSTRAP_SERVERS']
TOPIC = 'set_price_feed'
SET_TICKERS = [
    'PTT.BK', 'KBANK.BK', 'SCB.BK', 'ADVANC.BK', 'AOT.BK',
    'CPALL.BK', 'SCC.BK', 'GULF.BK', 'BDMS.BK', 'TRUE.BK',
    'DTAC.BK', 'MINT.BK', 'TOP.BK', 'BH.BK', 'CPN.BK',
    'HMPRO.BK', 'IVL.BK', 'KTB.BK', 'BBL.BK', 'PTTEP.BK',
]


def fetch_prices(is_correction: bool) -> list[dict]:
    today = datetime.now(timezone.utc).date()
    trade_date = (today - timedelta(days=1)) if is_correction else today
    data = yf.download(SET_TICKERS, period="1d", auto_adjust=True)
    events = []

    for ticker in SET_TICKERS:
        event = {
            "ticker": ticker,
            "trade_date": trade_date.isoformat(),
            "open":   float(data["Open"][ticker].iloc[0]),
            "high":   float(data["High"][ticker].iloc[0]),
            "low":    float(data["Low"][ticker].iloc[0]),
            "close":  float(data["Close"][ticker].iloc[0]),
            "volume": int(data["Volume"][ticker].iloc[0]),
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "is_correction": is_correction,
            "correction_for_date": (today - timedelta(days=1)).isoformat() if is_correction else None,
        }
        events.append(event)

    return events


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        compression_type='gzip',
        acks='all',
    )


def publish(producer: KafkaProducer, events: list[dict]) -> None:
    for event in events:
        producer.send(TOPIC, value=json.dumps(event).encode("utf-8"))


def run():
    is_correction = random.random() < 0.10
    events = fetch_prices(is_correction)
    producer = create_producer()
    publish(producer, events)
    producer.flush()


if __name__ == "__main__":
    run()
