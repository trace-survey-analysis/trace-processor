import re
import fitz  # PyMuPDF
from datetime import datetime
from typing import Dict, List, Any, Tuple, Optional
from models.data_models import (
    TraceUploadMessage,
    TraceProcessedMessage,
    Course,
    Instructor,
    Rating,
    Comment,
)
from utils.logging import Logger


class PDFExtractor:
    """Extracts structured data from TRACE survey PDFs"""

    def __init__(self, logger: Logger):
        self.logger = logger

    def extract(
        self, pdf_data: bytes, upload_message: TraceUploadMessage
    ) -> TraceProcessedMessage:
        """
        Extract data from a trace survey PDF

        Args:
            pdf_data: The PDF file as bytes
            upload_message: The original upload message containing metadata

        Returns:
            A processed message with extracted data
        """
        self.logger.info(
            "Starting PDF data extraction", "traceId", upload_message.traceId
        )

        # Open the PDF from the binary data
        doc = fitz.open(stream=pdf_data, filetype="pdf")

        # Extract text from each page individually
        text_by_page = []
        for page in doc:
            text_by_page.append(page.get_text())

        # Combine all text
        full_text = "\n".join(text_by_page)

        self.logger.info(f"Successfully extracted {len(full_text)} characters from PDF")

        # Extract course info
        course = self._extract_course_info(full_text, upload_message)

        # Extract instructor info
        instructor = self._extract_instructor_info(full_text)

        # Extract ratings
        ratings = self._extract_ratings(full_text)

        # Extract comments
        comments = self._extract_comments(text_by_page)

        # Create processed message
        processed_message = TraceProcessedMessage(
            traceId=upload_message.traceId,
            course=course,
            instructor=instructor,
            ratings=ratings,
            comments=comments,
            processedAt=datetime.now(),
        )

        self.logger.info(
            "Data extraction complete",
            "ratings",
            len(ratings),
            "comments",
            len(comments),
        )

        return processed_message

    def _extract_course_info(
        self, full_text: str, upload_message: TraceUploadMessage
    ) -> Course:
        """Extract course information from the PDF text"""
        # Split text by pages to get first page for header analysis
        text_lines = full_text.split("\n")
        first_page_text = "\n".join(text_lines[:50]) if text_lines else ""

        # COURSE NAME EXTRACTION - more robust approach
        course_name = "Unknown Course"

        # Look for course name in the header (first few lines)
        top_lines = "\n".join(text_lines[:10]) if text_lines else ""

        # Try to find the most obvious pattern first - course name at the top of the document
        header_match = re.search(r"^([^\n]+)", top_lines)
        if header_match and len(header_match.group(1).strip()) > 5:
            course_name = header_match.group(1).strip()

        # If that doesn't work, try searching for text followed by "(Fall 2024)" or similar
        if course_name == "Unknown Course":
            term_pattern = re.search(r"([^\n]+)\s*\(\w+\s+\d{4}\)", first_page_text)
            if term_pattern:
                course_name = term_pattern.group(1).strip()

        # Remove semester/year from course name if present
        course_name = re.sub(r"\s*\(\w+\s+\d{4}\)\s*$", "", course_name)

        # Try to extract course ID from the PDF or use the one from the upload message
        course_id_match = re.search(r"Course ID:?\s*(\d+)", full_text)
        course_id = (
            course_id_match.group(1) if course_id_match else upload_message.courseId
        )

        # Try to extract other fields
        subject_match = re.search(r"Subject:?\s*(\w+)", full_text)
        subject = subject_match.group(1) if subject_match else ""

        catalog_section_match = re.search(
            r"Catalog\s*&\s*Section:?\s*(\d+\s*\d+)", full_text
        )
        catalog_section = (
            catalog_section_match.group(1)
            if catalog_section_match
            else upload_message.section
        )

        enrollment_match = re.search(r"Enrollment:\s*(\d+)", full_text)
        enrollment = int(enrollment_match.group(1)) if enrollment_match else 0

        responses_match = re.search(r"Responses\s*Incl\s*Declines:\s*(\d+)", full_text)
        responses = int(responses_match.group(1)) if responses_match else 0

        declines_match = re.search(r"(?<!Incl )Declines:\s*(\d+)", full_text)
        declines = (
            int(declines_match.group(1)) if declines_match else 1
        )  # Default to 1 if not found

        # Try to extract semester and year directly from document
        semester = "Fall"  # Default
        year = datetime.now().year  # Default to current year

        # Look for semester/year pattern in the document (e.g., "(Fall 2022)")
        semester_year_match = re.search(r"\((\w+)\s+(\d{4})\)", full_text)
        if semester_year_match:
            semester = semester_year_match.group(1)
            year = int(semester_year_match.group(2))
        elif upload_message.semesterTerm:
            # Fall back to parsing from upload_message (e.g., "Fall-2022")
            semester_term_parts = upload_message.semesterTerm.split("-")
            if len(semester_term_parts) >= 1:
                semester = semester_term_parts[0]
            if len(semester_term_parts) >= 2:
                try:
                    year = int(semester_term_parts[1])
                except (ValueError, TypeError):
                    pass

        self.logger.info(
            f"Extracted course info: {course_name}, {semester} {year}, Subject: {subject}"
        )

        return Course(
            courseId=course_id,
            courseName=course_name,
            subject=subject,
            catalogSection=catalog_section,
            semester=semester,
            year=year,
            enrollment=enrollment,
            responses=responses,
            declines=declines,
            processedAt=datetime.now(),
            originalFileName=upload_message.fileName,
            gcsBucket=upload_message.gcsBucket,
            gcsPath=upload_message.gcsPath,
        )

    def _extract_instructor_info(self, full_text: str) -> Instructor:
        """Extract instructor information from the PDF text"""
        instructor_match = re.search(
            r"Instructor:\s*([\w\s,]+)(?=\s*\nSubject)", full_text
        )
        instructor_name = (
            instructor_match.group(1).strip() if instructor_match else "Unknown"
        )

        return Instructor(name=instructor_name)

    def _extract_ratings(self, full_text: str) -> List[Rating]:
        """Extract ratings from the PDF text"""
        # Complete question texts from the PDF (copied from test.py)
        question_texts = [
            "Online course materials were organized to help me navigate through the course week by week.",
            "Online interactions with my instructor created a sense of connection in the virtual classroom.",
            "Online course interactions created a sense of community and connection to my classmates.",
            "I had the necessary computer skills and technology to successfully complete the course.",
            "The syllabus was accurate and helpful in delineating expectations and course outcomes.",
            "Required and additional course materials were helpful in achieving course outcomes.",
            "In-class sessions were helpful for learning.",
            "Out-of-class assignments and/or fieldwork were helpful for learning.",
            "This course was intellectually challenging.",
            "I learned a lot in this course.",
            "The instructor came to class prepared to teach.",
            "The instructor used class time effectively.",
            "The instructor clearly communicated ideas and information.",
            "The instructor provided sufficient feedback.",
            "The instructor fairly evaluated my performance.",
            "The instructor was available to assist students outside of class.",
            "The instructor facilitated a respectful and inclusive learning environment.",
            "The instructor displayed enthusiasm for the course.",
            "What is your overall rating of this instructor's teaching effectiveness?",
        ]

        # Category mapping for questions
        question_categories = {
            "Online course materials were organized": "Online Experience",
            "Online interactions with my instructor": "Online Experience",
            "Online course interactions created": "Online Experience",
            "I had the necessary computer skills": "Online Experience",
            "The syllabus was accurate": "Course Related",
            "Required and additional course materials": "Course Related",
            "In-class sessions were": "Learning Related",
            "Out-of-class assignments": "Learning Related",
            "This course was intellectually": "Learning Related",
            "I learned a lot": "Learning Related",
            "The instructor came to class": "Instructor Related",
            "The instructor used class time": "Instructor Related",
            "The instructor clearly communicated": "Instructor Related",
            "The instructor provided sufficient": "Instructor Related",
            "The instructor fairly evaluated": "Instructor Related",
            "The instructor was available": "Instructor Related",
            "The instructor facilitated": "Instructor Related",
            "The instructor displayed": "Instructor Related",
            "What is your overall rating": "Instructor Effectiveness",
        }

        ratings = []

        for full_question in question_texts:
            # Find the category
            category = "Unknown"
            for key, cat in question_categories.items():
                if full_question.startswith(key):
                    category = cat
                    break

            # Find a numeric pattern that follows this question
            # Look for all instances of the pattern: number, percentage, 6 decimal numbers
            rating_pattern = r"(\d+)\s+(\d+)%\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)"

            # Try to find the question exactly or partially
            question_pos = full_text.find(full_question)
            if question_pos == -1:
                for key in question_categories.keys():
                    if full_question.startswith(key):
                        question_pos = full_text.find(key)
                        break

            if question_pos == -1:
                continue  # Skip if we can't find this question

            # Look for rating data after this question
            text_after = full_text[question_pos : question_pos + 300]
            rating_match = re.search(rating_pattern, text_after)

            if rating_match:
                rating = Rating(
                    questionText=full_question,
                    category=category,
                    responses=int(rating_match.group(1)),
                    responseRate=float(rating_match.group(2)) / 100,
                    courseMean=float(rating_match.group(3)),
                    deptMean=float(rating_match.group(4)),
                    univMean=float(rating_match.group(5)),
                    courseMedian=float(rating_match.group(6)),
                    deptMedian=float(rating_match.group(7)),
                    univMedian=float(rating_match.group(8)),
                )
                ratings.append(rating)

        return ratings

    def _extract_comments(self, text_by_page: List[str]) -> List[Comment]:
        """Extract comments from the PDF text"""
        comments = []

        # Define comment sections with known headers
        comment_sections = [
            (
                "What were the strengths of this course and/or this instructor?",
                "Strengths",
            ),
            (
                "What could the instructor do to make this course better?",
                "Improvements",
            ),
            (
                "Please expand on the instructor's strengths and/or areas for improvement in facilitating inclusive learning.",
                "Inclusive Learning",
            ),
            (
                "Please comment on your experience of the online course environment in the open-ended text box.",
                "Online Experience",
            ),
            (
                "What I could have done to make this course better for myself.",
                "Self-Assessment",
            ),
        ]

        # List of patterns to remove from comment text (metadata and section markers)
        cleanup_patterns = [
            r"Q:\s*[^\n]+",  # Q: followed by text
            r"Questions to Assess Students'[^\n]+",  # Section headers
            r"Student Self-Assessment[^\n]+",  # Section headers
            r"\(\d+\s*comments\)",  # Comment counts
        ]

        # Combine all pages into one text for easier processing
        full_text = "\n".join(text_by_page)

        # Find comment pages
        comment_page_idx = -1
        for page_idx, page_text in enumerate(text_by_page):
            if any(header[0] in page_text for header in comment_sections):
                comment_page_idx = page_idx
                break

        if comment_page_idx >= 0:
            # Get all text from comment pages
            comment_text = "\n".join(text_by_page[comment_page_idx:])

            # Process each comment section
            for header, category in comment_sections:
                # Find the section
                section_start = comment_text.find(header)
                if section_start == -1:
                    continue

                # Find the next section or end of text
                section_end = len(comment_text)
                for next_header, _ in comment_sections:
                    if next_header != header:
                        next_pos = comment_text.find(
                            next_header, section_start + len(header)
                        )
                        if next_pos != -1 and next_pos < section_end:
                            section_end = next_pos

                # Extract the section text
                section_text = comment_text[
                    section_start + len(header) : section_end
                ].strip()

                # Find numbered comments with improved pattern
                # Look for number at start of line, followed by text until next number or end
                numbered_comments = re.finditer(
                    r"(\d+)\s+(.*?)(?=\n\s*\d+\s+|\n\s*Q:|\n\s*Note:|\Z)",
                    section_text,
                    re.DOTALL,
                )

                for match in numbered_comments:
                    number = int(match.group(1))
                    text = match.group(2).strip()

                    # Skip very short text
                    if len(text) < 5:
                        continue

                    # Clean up the text - remove metadata markers and normalize whitespace
                    cleaned_text = text
                    for pattern in cleanup_patterns:
                        cleaned_text = re.sub(pattern, "", cleaned_text)

                    # Normalize whitespace (newlines to spaces, multiple spaces to single)
                    cleaned_text = re.sub(r"\s+", " ", cleaned_text).strip()

                    comments.append(
                        Comment(
                            category=category,
                            questionText=header,
                            responseNumber=number,
                            commentText=cleaned_text,
                        )
                    )

        return comments
