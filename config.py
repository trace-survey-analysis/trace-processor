import os
from typing import List, Dict, Any


class Config:
    """Configuration class that loads values from environment variables"""

    def __init__(self):
        # Server configuration
        self.server_port = self._get_env("SERVER_PORT", "8081")

        # Kafka configuration
        kafka_brokers_str = self._get_env(
            "KAFKA_BROKERS",
            "kafka-controller-0.kafka-controller-headless.kafka.svc.cluster.local:9092,"
            "kafka-controller-1.kafka-controller-headless.kafka.svc.cluster.local:9092,"
            "kafka-controller-2.kafka-controller-headless.kafka.svc.cluster.local:9092",
        )
        self.kafka_brokers = kafka_brokers_str.split(",")
        self.input_topic = self._get_env("KAFKA_INPUT_TOPIC", "trace-survey-uploaded")
        self.output_topic = self._get_env(
            "KAFKA_OUTPUT_TOPIC", "trace-survey-processed"
        )
        self.consumer_group = self._get_env("KAFKA_CONSUMER_GROUP", "")
        self.kafka_username = self._get_env("KAFKA_USERNAME", "")
        self.kafka_password = self._get_env("KAFKA_PASSWORD", "")
        self.kafka_auth = bool(self.kafka_username and self.kafka_password)

        # Retry configuration
        self.max_retries = int(self._get_env("MAX_RETRIES", "3"))
        self.retry_backoff_ms = int(self._get_env("RETRY_BACKOFF_MS", "1000"))

        # PDF processing configuration
        self.max_concurrent_jobs = int(self._get_env("MAX_CONCURRENT_JOBS", "5"))
        self.processing_timeout = int(self._get_env("PROCESSING_TIMEOUT", "300"))

    def _get_env(self, key: str, default: str) -> str:
        """Get an environment variable or return a default value"""
        return os.environ.get(key, default)

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to a dictionary"""
        return {
            "server_port": self.server_port,
            "kafka_brokers": self.kafka_brokers,
            "input_topic": self.input_topic,
            "output_topic": self.output_topic,
            "consumer_group": self.consumer_group,
            "kafka_auth": self.kafka_auth,
            "max_retries": self.max_retries,
            "retry_backoff_ms": self.retry_backoff_ms,
            "max_concurrent_jobs": self.max_concurrent_jobs,
            "processing_timeout": self.processing_timeout,
        }


def load_config() -> Config:
    """Load and return configuration"""
    return Config()
