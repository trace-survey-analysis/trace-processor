import time
from typing import Optional
from google.cloud import storage
from utils.logging import Logger


class GCSClient:
    """Client for interacting with Google Cloud Storage"""

    def __init__(self, logger: Logger):
        """Initialize the GCS client"""
        self.client = storage.Client()
        self.retry_count = 3  # Default retry count
        self.logger = logger

    def download_file(self, bucket_name: str, path: str) -> bytes:
        """
        Download a file from GCS with retries

        Args:
            bucket_name: The name of the bucket
            path: The path to the file within the bucket

        Returns:
            The file contents as bytes

        Raises:
            Exception: If the file cannot be downloaded after retries
        """
        attempts = 0

        # Retry loop with exponential backoff
        while attempts < self.retry_count:
            try:
                self.logger.info(
                    "Attempting to download file from GCS",
                    "bucket",
                    bucket_name,
                    "path",
                    path,
                    "attempt",
                    attempts + 1,
                )

                bucket = self.client.bucket(bucket_name)
                blob = bucket.blob(path)
                data = blob.download_as_bytes()

                self.logger.info("File downloaded successfully", "size", len(data))
                return data

            except Exception as e:
                attempts += 1
                if attempts >= self.retry_count:
                    self.logger.error(
                        f"Failed to download file after {attempts} attempts",
                        e,
                        "bucket",
                        bucket_name,
                        "path",
                        path,
                    )
                    raise

                # Wait before retrying with exponential backoff
                backoff_time = (attempts * 500) / 1000.0  # Convert to seconds
                self.logger.info(
                    f"Retrying download in {backoff_time}s", "attempt", attempts + 1
                )
                time.sleep(backoff_time)

        # This shouldn't be reached due to the raise above, but just in case
        raise Exception(f"Failed to download file after {self.retry_count} attempts")
