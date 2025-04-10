import json
import time
from typing import Callable, Dict, Any, Optional
from datetime import datetime
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from models.data_models import TraceUploadMessage, TraceProcessedMessage
from utils.logging import Logger


class KafkaConsumer:
    """Kafka consumer for receiving trace upload messages"""

    def __init__(
        self,
        brokers: list,
        topic: str,
        group_id: str,
        username: str,
        password: str,
        enable_auth: bool,
        handler: Callable,
        logger: Logger,
        max_retries: int = 3,
        retry_backoff_ms: int = 1000,
    ):
        """Initialize the Kafka consumer"""
        self.brokers = brokers
        self.topic = topic
        self.group_id = group_id
        self.username = username
        self.password = password
        self.enable_auth = enable_auth
        self.handler = handler
        self.logger = logger
        self.max_retries = max_retries
        self.retry_backoff_ms = retry_backoff_ms
        self.consumer = self._create_consumer()
        self.running = False

    def _create_consumer(self) -> Consumer:
        """Create a Kafka consumer with the appropriate configuration"""
        config = {
            "bootstrap.servers": ",".join(self.brokers),
            "auto.offset.reset": "latest" if not self.group_id else "earliest",
            "enable.auto.commit": False,
            "session.timeout.ms": 180000,  # 3 minutes, matching Go config
            "max.poll.interval.ms": 300000,  # 5 minutes, matching Go config
        }

        if self.group_id:
            config["group.id"] = self.group_id

        if self.enable_auth:
            if not self.username or not self.password:
                raise ValueError("Kafka authentication enabled but missing credentials")

            self.logger.info(
                "Setting up SASL PLAIN authentication", "username", self.username
            )

            config.update(
                {
                    "security.protocol": "SASL_PLAINTEXT",
                    "sasl.mechanisms": "PLAIN",
                    "sasl.username": self.username,
                    "sasl.password": self.password,
                }
            )

        self.logger.info(
            "Creating Kafka consumer",
            "brokers",
            self.brokers,
            "topic",
            self.topic,
            "groupID",
            self.group_id,
        )

        consumer = Consumer(config)
        consumer.subscribe([self.topic])

        return consumer

    def _reconnect(self) -> None:
        """Attempt to reconnect the consumer"""
        self.logger.info("Beginning reconnection process...")

        if self.consumer:
            self.logger.info("Closing existing consumer")
            self.consumer.close()

        self.logger.info("Creating new consumer connection...")
        self.consumer = self._create_consumer()
        self.logger.info("Successfully reconnected to Kafka with new consumer")

    def consume(self) -> None:
        """Consume messages from Kafka"""
        self.logger.info(
            "Starting to consume messages",
            "topic",
            self.topic,
            "groupID",
            self.group_id,
            "brokers",
            self.brokers,
        )

        reconnect_attempts = 0
        max_reconnect_attempts = 15
        reconnect_backoff = 0.5  # seconds

        self.running = True

        while self.running:
            try:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        self.logger.info(f"Reached end of partition {msg.partition()}")
                        continue
                    else:
                        self.logger.error(f"Consumer error: {msg.error()}")

                        # Attempt reconnect
                        if reconnect_attempts < max_reconnect_attempts:
                            reconnect_attempts += 1
                            time.sleep(reconnect_backoff)
                            reconnect_backoff *= 2
                            if reconnect_backoff > 10:
                                reconnect_backoff = 10

                            self._reconnect()
                        else:
                            self.logger.error(
                                "Maximum reconnect attempts reached, will retry in 30 seconds"
                            )
                            time.sleep(30)
                            reconnect_attempts = 0
                            reconnect_backoff = 0.5

                        continue

                # Reset reconnect attempts on successful message
                reconnect_attempts = 0
                reconnect_backoff = 0.5

                # Process message
                value = msg.value()
                self.logger.info(
                    "Received message",
                    "topic",
                    msg.topic(),
                    "partition",
                    msg.partition(),
                    "offset",
                    msg.offset(),
                    "length",
                    len(value) if value else 0,
                )

                if value:
                    try:
                        # Parse message
                        message_dict = json.loads(value)

                        # Convert string timestamps to datetime objects
                        if "uploadedAt" in message_dict and isinstance(
                            message_dict["uploadedAt"], str
                        ):
                            try:
                                # Try to use dateutil parser which is more flexible with formats
                                from dateutil import parser

                                message_dict["uploadedAt"] = parser.parse(
                                    message_dict["uploadedAt"]
                                )
                            except (ImportError, Exception) as e:
                                # Fallback method if dateutil is not available
                                timestamp = message_dict["uploadedAt"]
                                # Normalize format to something fromisoformat can handle
                                # Truncate microseconds to 6 digits if longer
                                if "." in timestamp:
                                    parts = timestamp.split(".")
                                    # If microseconds part is longer than 6 digits before timezone
                                    second_part = parts[1]
                                    if "+" in second_part:
                                        micro, tz = second_part.split("+", 1)
                                        if len(micro) > 6:
                                            micro = micro[:6]
                                        second_part = f"{micro}+{tz}"
                                    elif "-" in second_part:
                                        micro, tz = second_part.split("-", 1)
                                        if len(micro) > 6:
                                            micro = micro[:6]
                                        second_part = f"{micro}-{tz}"
                                    elif "Z" in second_part:
                                        micro = second_part.replace("Z", "")
                                        if len(micro) > 6:
                                            micro = micro[:6]
                                        second_part = f"{micro}Z"
                                    else:
                                        if len(second_part) > 6:
                                            second_part = second_part[:6]

                                    timestamp = f"{parts[0]}.{second_part}"

                                # Replace Z with +00:00 if present
                                timestamp = timestamp.replace("Z", "+00:00")

                                # Parse with modified timestamp
                                try:
                                    message_dict["uploadedAt"] = datetime.fromisoformat(
                                        timestamp
                                    )
                                except ValueError:
                                    # If still failing, use a simpler approach
                                    self.logger.warn(
                                        f"Failed to parse timestamp: {message_dict['uploadedAt']}, using current time"
                                    )
                                    message_dict["uploadedAt"] = datetime.now()

                        upload_msg = TraceUploadMessage(**message_dict)

                        # Process message with retries
                        process_error = None
                        for retry in range(self.max_retries + 1):
                            if retry > 0:
                                sleep_time = self.retry_backoff_ms / 1000.0
                                self.logger.info(
                                    f"Retrying message processing in {sleep_time}s",
                                    "attempt",
                                    retry,
                                    "traceId",
                                    upload_msg.traceId,
                                )
                                time.sleep(sleep_time)

                            try:
                                self.handler(upload_msg)
                                process_error = None
                                break
                            except Exception as e:
                                process_error = e
                                self.logger.error(
                                    f"Error processing message: {str(e)}",
                                    e,
                                    "attempt",
                                    retry,
                                    "traceId",
                                    upload_msg.traceId,
                                )

                        # Commit the message
                        self.logger.info("Committing message", "offset", msg.offset())
                        self.consumer.commit(message=msg, asynchronous=False)

                        if process_error:
                            self.logger.error(
                                "Failed to process message after max retries",
                                process_error,
                                "traceId",
                                upload_msg.traceId,
                            )
                        else:
                            self.logger.info(
                                "Successfully processed message",
                                "traceId",
                                upload_msg.traceId,
                            )
                    except json.JSONDecodeError as e:
                        self.logger.error("Error decoding message JSON", e)
                        # Commit invalid message to avoid getting stuck
                        self.consumer.commit(message=msg, asynchronous=False)
                    except Exception as e:
                        self.logger.error("Error processing message", e)
                        # Commit the message even if processing failed
                        self.consumer.commit(message=msg, asynchronous=False)
            except KafkaException as e:
                self.logger.error("Kafka exception", e)
                time.sleep(1)  # Avoid tight loop
            except Exception as e:
                self.logger.error("Unexpected exception", e)
                time.sleep(1)  # Avoid tight loop

    def close(self) -> None:
        """Close the Kafka consumer"""
        self.logger.info("Closing Kafka consumer")
        self.running = False
        if self.consumer:
            self.consumer.close()


class KafkaProducer:
    """Kafka producer for sending processed trace messages"""

    def __init__(
        self,
        brokers: list,
        topic: str,
        username: str,
        password: str,
        enable_auth: bool,
        logger: Logger,
    ):
        """Initialize the Kafka producer"""
        self.brokers = brokers
        self.topic = topic
        self.username = username
        self.password = password
        self.enable_auth = enable_auth
        self.logger = logger
        self.producer = self._create_producer()

    def _create_producer(self) -> Producer:
        """Create a Kafka producer with the appropriate configuration"""
        config = {
            "bootstrap.servers": ",".join(self.brokers),
            "acks": "all",  # Equivalent to RequireAll in Go
            "message.timeout.ms": 10000,  # 10 seconds, matching Go config
        }

        if self.enable_auth:
            if not self.username or not self.password:
                raise ValueError("Kafka authentication enabled but missing credentials")

            config.update(
                {
                    "security.protocol": "SASL_PLAINTEXT",
                    "sasl.mechanisms": "PLAIN",
                    "sasl.username": self.username,
                    "sasl.password": self.password,
                }
            )

        return Producer(config)

    def _delivery_report(self, err, msg) -> None:
        """Callback for message delivery"""
        if err is not None:
            self.logger.error(f"Message delivery failed: {err}")
        else:
            self.logger.info(
                f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
            )

    def publish_processed_message(self, message: TraceProcessedMessage) -> None:
        """Publish a processed trace survey message to Kafka"""
        try:
            # Convert message to JSON-serializable dict
            message_dict = message.model_dump()

            # Convert datetime objects to ISO format strings
            if "processedAt" in message_dict and isinstance(
                message_dict["processedAt"], datetime
            ):
                message_dict["processedAt"] = message_dict["processedAt"].isoformat()

            if (
                "course" in message_dict
                and "processedAt" in message_dict["course"]
                and isinstance(message_dict["course"]["processedAt"], datetime)
            ):
                message_dict["course"]["processedAt"] = message_dict["course"][
                    "processedAt"
                ].isoformat()

            data = json.dumps(message_dict).encode("utf-8")

            self.producer.produce(
                topic=self.topic,
                key=message.traceId.encode("utf-8"),
                value=data,
                headers={
                    "content-type": "application/json",
                    "source": "trace-processor",
                },
                callback=self._delivery_report,
            )

            # Wait for any outstanding messages to be delivered and delivery reports received
            self.producer.flush()

            self.logger.info(
                "Published processed trace message to Kafka", "traceId", message.traceId
            )
        except Exception as e:
            self.logger.error("Error publishing message to Kafka", e)
            raise

    def close(self) -> None:
        """Close the Kafka producer"""
        self.logger.info("Closing Kafka producer")
        if self.producer:
            self.producer.flush()
