import asyncio
import json

from aiokafka import AIOKafkaProducer

from flowcore_story.config import config as app_config

KAFKA_TOPIC = app_config.KAFKA_TOPIC
KAFKA_BOOTSTRAP_SERVERS = app_config.KAFKA_BOOTSTRAP_SERVERS

sample_jobs = {
    "full_site": {
        "type": "full_site",
        "site_key": "metruyenfull"
    },
    "genres_only": {
        "type": "full_site",
        "site_key": "truyenfull",
        "crawl_mode": "genres_only"
    },
    "missing_only": {
        "type": "full_site",
        "site_key": "truyenyy",
        "crawl_mode": "missing_only"
    },
    "retry_failed_genres": {
        "type": "retry_failed_genres",
        "site_key": "truyenfull"
    },
}

async def send_job(job_key: str):
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        job = sample_jobs.get(job_key)
        if not job:
            print(f"[ERROR] Job `{job_key}` không tồn tại trong sample_jobs.")
            return
        value = json.dumps(job).encode("utf-8")
        await producer.send_and_wait(KAFKA_TOPIC, value)
        print(f"[✅] Sent job `{job_key}` to topic `{KAFKA_TOPIC}`.")
    finally:
        await producer.stop()

if __name__ == "__main__":
    import sys
    job_key = sys.argv[1] if len(sys.argv) > 1 else "full_site"
    asyncio.run(send_job(job_key))
