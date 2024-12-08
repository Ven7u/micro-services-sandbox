import io
import json
import logging
import time

import httpx
import minio
import redis
import structlog
from confluent_kafka import Consumer, Producer

logging.basicConfig(level=logging.INFO)
logger = structlog.getLogger()

BAD_WORDS = ["badword"]

# Initialize MinIO client
MINIO_URL = "http://minio:9000"  # Replace with your MinIO URL
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET_NAME = "movie-review"

minio_client = minio.Minio(
    MINIO_URL.replace("http://", "").replace("https://", ""),
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_URL.startswith("https"),
)

KAFKA_CONSUMER_CONF = {
    "bootstrap.servers": "kafka:9092",
    "group.id": "enrichment-service",
    "auto.offset.reset": "earliest",
}

KAFKA_PRODUCER_CONF = {"bootstrap.servers": "kafka:9092"}

# Redis connection
r = redis.StrictRedis(host="redis", port=6379, db=0, decode_responses=True)


def update_movie_average_score(movie_title, movie_score) -> dict:
    new_data = {}
    current_data = r.hgetall(movie_title)
    logger.info(f"Updating average score", current_data=current_data)
    if not current_data:
        new_data = {"count": 1, "sum": movie_score, "average_score": movie_score}
        r.hset(name=movie_title, mapping=new_data)
    else:
        curr_count = int(str(current_data["count"]))
        curr_sum = float(str(current_data["sum"]))
        new_count = curr_count + 1
        new_sum = curr_sum + movie_score
        new_average = new_sum / new_count

        new_data = {"count": new_count, "sum": new_sum, "average_score": new_average}
        r.hset(name=movie_title, mapping=new_data)

    return new_data


def save_to_minio(key: str, data: dict):
    """
    Save the given data as a JSON object in MinIO under the specified key.
    """
    try:
        # Convert the data to JSON
        json_data = json.dumps(data)

        logger.info(f"Saving data in analytics bucket", key=key, data=json_data)
        json_data_b = json_data.encode("utf-8")

        # Put the object into MinIO
        minio_client.put_object(
            MINIO_BUCKET_NAME,
            key,
            data=io.BytesIO(json_data_b),
            length=len(json_data_b),
            content_type="application/json",
        )
        logger.info(
            f"Data saved into analytics bucket: {key}",
            key=key,
        )
    except minio.S3Error as s3_error:
        logger.error(f"MinIO error: {s3_error}")
        raise
    except Exception as ex:
        logger.error(f"Error saving data to MinIO | Exception: {ex}")
        raise


def is_compliant(message: dict) -> bool:
    if any([w in message["review"] for w in BAD_WORDS]):
        return False

    return True


class MessageNotCompliantException(Exception):
    pass


class MessageProcessingException(Exception):
    pass


def process_message(message: dict):

    # Messages are partitioned by movie so lock is not required on per movie operations

    # Check compliance
    if not is_compliant(message):
        raise MessageNotCompliantException("Message is not compliant")

    # Save the review
    response = httpx.post("http://api:8081/reviews", json=message, verify=False)
    logger.info(
        "Saving review",
        status_code=response.status_code,
        response=str(response.content),
        review=message,
    )

    # Compute the new movie average score
    new_average_score = update_movie_average_score(message["title"], message["score"])

    # Compute the new top words
    # ...

    # Save the updated movie stats
    updated_movie = {
        "movie_id": message["movie_id"],
        "title": message["title"],
        "average_score": float(new_average_score["average_score"]),
    }
    response = httpx.post(
        "http://api:8081/movies",
        json=updated_movie,
        verify=False,
    )
    logger.info(
        "Updating movie",
        status_code=response.status_code,
        response=str(response.content),
        movie=updated_movie,
    )

    # Save the reivew and stats into the analytics store (MINIO) for furter analytics/monitoring
    review_key = f"reviews/{message['title']}/{message['review_id']}.json"
    save_to_minio(review_key, message)


def create_kafka_consumer() -> Consumer:
    return Consumer(KAFKA_CONSUMER_CONF)


def create_kafka_producer() -> Producer:
    return Producer(KAFKA_PRODUCER_CONF)


def run_worker():
    consumer = create_kafka_consumer()
    consumer.subscribe(["ingestion-movie-reviews"])
    producer = create_kafka_producer()  # Producer for the Dead Letter Queue

    max_retries = 3  # Retry limit for transient errors

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue

            # Retry logic for transient errors
            attempt = 0
            while attempt < max_retries:
                try:
                    # Decode the message
                    message = json.loads(msg.value().decode("utf-8"))
                    logger.info(f"Processing message: {message}")

                    # Do the actual processing
                    process_message(message)

                    # Commit on success
                    consumer.commit(asynchronous=False)
                    break

                except MessageNotCompliantException:
                    # Discard non-compliant messages
                    logger.info(f"Non-compliant message discarded: {message}")
                    consumer.commit(asynchronous=False)  # Commit to skip the message
                    break

                except Exception as ex:
                    logger.exception(f"Error processing message: {ex}")
                    logger.warning(
                        f"Retry {attempt + 1} failed for message: {message}."
                    )
                    time.sleep(1.8**attempt)  # Exponential backoff
                    attempt += 1

            if attempt >= max_retries:
                try:
                    # All retries failed, send to DLQ
                    logger.error(
                        f"Message processing failed after {max_retries} attempts. Sending to DLQ."
                    )
                    producer.produce(
                        topic="dead-letter-queue",
                        value=msg.value(),
                    )
                    producer.flush()  # Ensure DLQ message is sent

                except Exception as ex:
                    # Handle unexpected errors without committing
                    logger.error(
                        f"Unexpected error, can not send message to DLQ: {ex}."
                    )

    except KeyboardInterrupt:
        logger.info("Worker interrupted. Shutting down...")
    except Exception as ex:
        logger.exception(f"Unhendled exception: {ex}")
    finally:
        consumer.close()


if __name__ == "__main__":
    run_worker()
