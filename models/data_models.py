from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field


class Comment(BaseModel):
    """Represents a student comment from a trace survey"""

    category: str
    questionText: str
    responseNumber: int
    commentText: str


class Instructor(BaseModel):
    """Represents an instructor from a trace survey"""

    name: str


class Rating(BaseModel):
    """Represents a rating question and response from a trace survey"""

    questionText: str
    category: str
    responses: int
    responseRate: float
    courseMean: float
    deptMean: float
    univMean: float
    courseMedian: float
    deptMedian: float
    univMedian: float


class Course(BaseModel):
    """Represents a course from a trace survey"""

    courseId: str
    courseName: str
    subject: str
    catalogSection: str
    semester: str
    year: int
    enrollment: int
    responses: int
    declines: int
    processedAt: datetime
    originalFileName: str
    gcsBucket: str
    gcsPath: str


class TraceUploadMessage(BaseModel):
    """Represents a message received from the Kafka topic"""

    traceId: str
    courseId: str
    fileName: str
    gcsBucket: str
    gcsPath: str
    instructorId: str
    semesterTerm: str
    section: str
    uploadedBy: str
    uploadedAt: datetime


class TraceProcessedMessage(BaseModel):
    """Represents a message to be sent to the output Kafka topic"""

    traceId: str
    course: Course
    instructor: Instructor
    ratings: List[Rating]
    comments: List[Comment]
    processedAt: datetime
    error: Optional[str] = None
