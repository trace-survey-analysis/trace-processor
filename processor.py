from typing import List, Dict, Any, Optional
from services.gcs_service import GCSClient
from services.kafka_service import KafkaProducer
from services.pdf_extractor import PDFExtractor
from models.data_models import TraceUploadMessage, TraceProcessedMessage
from utils.logging import Logger


class SurveyProcessor:
    """Processes TRACE surveys by extracting data from PDFs and publishing results"""

    def __init__(self, gcs_client: GCSClient, producer: KafkaProducer, logger: Logger):
        """Initialize the survey processor"""
        self.gcs_client = gcs_client
        self.producer = producer
        self.pdf_extractor = PDFExtractor(logger)
        self.logger = logger

    def process_message(self, message: TraceUploadMessage) -> None:
        """
        Process a trace survey upload message

        Args:
            message: The upload message containing GCS file information

        Raises:
            Exception: If there is an error during processing
        """
        self.logger.info("Processing trace upload message", "traceId", message.traceId)

        try:
            # Download the PDF from GCS
            self.logger.info(
                "Downloading PDF from GCS",
                "bucket",
                message.gcsBucket,
                "path",
                message.gcsPath,
            )
            pdf_data = self.gcs_client.download_file(message.gcsBucket, message.gcsPath)
            self.logger.info("Downloaded PDF successfully", "size", len(pdf_data))

            # Extract data from the PDF
            self.logger.info("Extracting data from PDF")
            try:
                processed_msg = self.pdf_extractor.extract(pdf_data, message)
                self.logger.info(
                    "Extracted data successfully",
                    "ratings",
                    len(processed_msg.ratings),
                    "comments",
                    len(processed_msg.comments),
                )
            except Exception as e:
                self.logger.error(f"PDF extraction error: {str(e)}", e)
                # Create a minimal processed message with error info
                from datetime import datetime
                from models.data_models import TraceProcessedMessage, Course, Instructor

                processed_msg = TraceProcessedMessage(
                    traceId=message.traceId,
                    course=Course(
                        courseId=message.courseId,
                        courseName="Error Processing Course",
                        subject="",
                        catalogSection=message.section,
                        semester="",
                        year=datetime.now().year,
                        enrollment=0,
                        responses=0,
                        declines=0,
                        processedAt=datetime.now(),
                        originalFileName=message.fileName,
                        gcsBucket=message.gcsBucket,
                        gcsPath=message.gcsPath,
                    ),
                    instructor=Instructor(name="Unknown"),
                    ratings=[],
                    comments=[],
                    processedAt=datetime.now(),
                    error=f"Extraction failed: {str(e)}",
                )

            # Validate the extracted data
            try:
                self._validate_processed_message(processed_msg)
            except ValueError as e:
                self.logger.warn(f"Validation warning: {str(e)}")
                processed_msg.error = (
                    f"{processed_msg.error or ''} Validation issue: {str(e)}".strip()
                )

            # Publish the processed message to Kafka
            self.logger.info("Publishing processed message to Kafka")
            self.producer.publish_processed_message(processed_msg)
            self.logger.info("Published processed message successfully")

        except Exception as e:
            self.logger.error(
                f"Error processing message: {str(e)}", e, "traceId", message.traceId
            )
            raise

    def _validate_processed_message(self, msg: TraceProcessedMessage) -> None:
        """Validate the processed message"""
        # Validate course information
        if not msg.course.courseId:
            raise ValueError("course ID is required")
        if not msg.course.courseName:
            raise ValueError("course name is required")
        if not msg.course.subject:
            raise ValueError("subject is required")

        # Validate instructor information
        if not msg.instructor.name:
            raise ValueError("instructor name is required")

        # Validate ratings
        if len(msg.ratings) == 0:
            self.logger.warn("No ratings found in processed message")
            # Not returning an error as some surveys might not have ratings

        # Validate comments
        if len(msg.comments) == 0:
            self.logger.warn("No comments found in processed message")
            # Not returning an error as some surveys might not have comments
