# TRACE Survey Processor

This service consumes messages from a Kafka topic, downloads PDF files from Google Cloud Storage, extracts data from these PDFs, and publishes the processed data to another Kafka topic.

## Overview

The TRACE Survey Processor application is responsible for:

1. Consuming trace survey upload notifications from the `trace-survey-uploaded` Kafka topic
2. Downloading the corresponding PDF files from Google Cloud Storage (GCS)
3. Extracting structured data from the PDFs (course info, instructor, ratings, and comments)
4. Publishing the processed data to the `trace-survey-processed` Kafka topic

## Architecture

The service consists of the following components:

- **Kafka Consumer**: Consumes messages from the `trace-survey-uploaded` topic
- **GCS Client**: Downloads PDF files from Google Cloud Storage
- **PDF Extractor**: Extracts structured data from the survey PDFs
- **Kafka Producer**: Publishes processed data to the `trace-survey-processed` topic
- **Health Check Server**: Provides endpoints for Kubernetes liveness and readiness probes

## Data Flow

1. A message is received from the `trace-survey-uploaded` Kafka topic containing the trace ID and GCS file location
2. The PDF file is downloaded from GCS
3. The PDF contents are parsed to extract:
   - Course information (ID, name, subject, etc.)
   - Instructor information
   - Ratings (question text, means, medians, etc.)
   - Student comments
4. The extracted data is published to the `trace-survey-processed` Kafka topic

## Configuration

The service is configured using environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `SERVER_PORT` | HTTP server port | `8081` |
| `KAFKA_BROKERS` | Comma-separated list of Kafka brokers | `kafka-controller-0...` |
| `KAFKA_INPUT_TOPIC` | Topic to consume messages from | `trace-survey-uploaded` |
| `KAFKA_OUTPUT_TOPIC` | Topic to publish processed messages to | `trace-survey-processed` |
| `KAFKA_CONSUMER_GROUP` | Consumer group ID | `trace-processor` |
| `KAFKA_USERNAME` | Kafka username for SASL auth | `""` |
| `KAFKA_PASSWORD` | Kafka password for SASL auth | `""` |
| `MAX_RETRIES` | Maximum number of processing retries | `3` |
| `RETRY_BACKOFF_MS` | Backoff time in ms between retries | `1000` |
| `MAX_CONCURRENT_JOBS` | Maximum concurrent processing jobs | `5` |
| `PROCESSING_TIMEOUT` | Processing timeout in seconds | `300` |
| `LOG_LEVEL` | Logging level (debug, info, warn, error, fatal) | `info` |

## Health Checks

The application provides the following health check endpoints:

- `/healthz/live`: Liveness probe to check if the application is running
- `/healthz/ready`: Readiness probe to check if the application is ready to process requests, including connectivity to Kafka and GCS

## Development

### Prerequisites

- Python 3.10+
- pipenv (optional)

### Setup

1. Clone the repository
2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

### Running Locally

To run the service locally:

```
python app.py
```

## Deployment

### Building the Docker Image

```
docker build -t trace-processor:latest .
```

### Running with Docker

```
docker run -p 8081:8081 \
  -e KAFKA_BROKERS=kafka:9092 \
  -e KAFKA_CONSUMER_GROUP=trace-processor \
  trace-processor:latest
```

### Kubernetes Deployment

The service can be deployed to Kubernetes using Helm:

```
helm upgrade --install trace-processor ./helm/trace-processor
```

## Message Format

### Input Message (trace-survey-uploaded)

```json
{
  "traceId": "string",
  "courseId": "string",
  "fileName": "string",
  "gcsBucket": "string",
  "gcsPath": "string",
  "instructorId": "string",
  "semesterTerm": "string",
  "section": "string",
  "uploadedBy": "string",
  "uploadedAt": "2023-01-01T00:00:00Z"
}
```

### Output Message (trace-survey-processed)

```json
{
  "traceId": "string",
  "course": {
    "courseId": "string",
    "courseName": "string",
    "subject": "string",
    "catalogSection": "string",
    "semester": "string",
    "year": 2023,
    "enrollment": 25,
    "responses": 20,
    "declines": 1,
    "processedAt": "2023-01-01T00:00:00Z",
    "originalFileName": "string",
    "gcsBucket": "string",
    "gcsPath": "string"
  },
  "instructor": {
    "name": "string"
  },
  "ratings": [
    {
      "questionText": "string",
      "category": "string",
      "responses": 20,
      "responseRate": 0.8,
      "courseMean": 4.5,
      "deptMean": 4.2,
      "univMean": 4.0,
      "courseMedian": 5.0,
      "deptMedian": 4.0,
      "univMedian": 4.0
    }
  ],
  "comments": [
    {
      "category": "string",
      "questionText": "string",
      "responseNumber": 1,
      "commentText": "string"
    }
  ],
  "processedAt": "2023-01-01T00:00:00Z",
  "error": "string"
}
```