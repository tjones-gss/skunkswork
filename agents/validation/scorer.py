"""
Scorer Agent
NAM Intelligence Pipeline

Assigns quality scores (0-100) to company records.
"""

from datetime import datetime, timedelta, UTC
from typing import Optional

from agents.base import BaseAgent


class ScorerAgent(BaseAgent):
    """
    Quality Scorer Agent - assigns quality scores.

    Responsibilities:
    - Calculate completeness score
    - Calculate accuracy score (from validated fields)
    - Calculate freshness score
    - Calculate source reliability score
    - Assign final quality score and letter grade
    """

    # Required fields (must have)
    REQUIRED_FIELDS = [
        "company_name",
        "website",
        "city",
        "state",
    ]

    # Valuable fields (nice to have)
    VALUABLE_FIELDS = [
        "employee_count_min",
        "revenue_min_usd",
        "erp_system",
        "contacts",
        "year_founded",
        "naics_code",
        "industry",
        "phone",
        "email",
    ]

    # Source reliability scores
    SOURCE_SCORES = {
        "clearbit": 95,
        "zoominfo": 90,
        "apollo": 85,
        "builtwith": 80,
        "website": 70,
        "job_postings": 65,
        "association": 60,
        "unknown": 50,
    }

    def _setup(self, **kwargs):
        """Initialize scorer settings."""
        self.min_quality_score = self.agent_config.get("min_quality_score", 60)
        self.weights = self.agent_config.get("weights", {
            "completeness": 0.30,
            "accuracy": 0.40,
            "freshness": 0.15,
            "source_reliability": 0.15
        })

        # Override defaults from config
        if "required_fields" in self.agent_config:
            self.REQUIRED_FIELDS = self.agent_config["required_fields"]
        if "valuable_fields" in self.agent_config:
            self.VALUABLE_FIELDS = self.agent_config["valuable_fields"]
        if "source_scores" in self.agent_config:
            self.SOURCE_SCORES = self.agent_config["source_scores"]

    async def run(self, task: dict) -> dict:
        """
        Score records.

        Args:
            task: {
                "records": [{...}, ...]
            }

        Returns:
            {
                "success": True,
                "records": [{...scored...}, ...],
                "quality_distribution": {...}
            }
        """
        records = task.get("records", [])

        if not records:
            return {
                "success": False,
                "error": "No records provided",
                "records": [],
                "records_processed": 0
            }

        self.log.info(f"Scoring {len(records)} records")

        scored_records = []
        quality_dist = {"A": 0, "B": 0, "C": 0, "D": 0, "F": 0}
        scores = []

        for record in records:
            # Calculate component scores
            completeness = self._calculate_completeness(record)
            accuracy = self._calculate_accuracy(record)
            freshness = self._calculate_freshness(record)
            source_reliability = self._calculate_source_reliability(record)

            # Calculate weighted final score
            final_score = (
                completeness * self.weights.get("completeness", 0.30) +
                accuracy * self.weights.get("accuracy", 0.40) +
                freshness * self.weights.get("freshness", 0.15) +
                source_reliability * self.weights.get("source_reliability", 0.15)
            )

            # Ensure score is in valid range
            final_score = max(0, min(100, round(final_score)))

            # Assign letter grade
            grade = self._get_grade(final_score)

            # Update record
            record["quality_score"] = final_score
            record["quality_grade"] = grade
            record["_quality_components"] = {
                "completeness": round(completeness, 1),
                "accuracy": round(accuracy, 1),
                "freshness": round(freshness, 1),
                "source_reliability": round(source_reliability, 1)
            }
            record["scored_at"] = datetime.now(UTC).isoformat()

            scored_records.append(record)
            quality_dist[grade] += 1
            scores.append(final_score)

        # Calculate statistics
        avg_score = sum(scores) / len(scores) if scores else 0
        median_score = sorted(scores)[len(scores) // 2] if scores else 0

        self.log.info(
            f"Scoring complete",
            average_score=round(avg_score, 1),
            median_score=median_score,
            quality_distribution=quality_dist
        )

        return {
            "success": True,
            "records": scored_records,
            "quality_distribution": quality_dist,
            "average_score": round(avg_score, 1),
            "median_score": median_score,
            "records_processed": len(records)
        }

    def _calculate_completeness(self, record: dict) -> float:
        """Calculate completeness score (0-100)."""
        # Required fields (weight: 60%)
        required_filled = sum(
            1 for field in self.REQUIRED_FIELDS
            if self._has_value(record.get(field))
        )
        required_score = (required_filled / len(self.REQUIRED_FIELDS)) * 60

        # Valuable fields (weight: 40%)
        valuable_filled = sum(
            1 for field in self.VALUABLE_FIELDS
            if self._has_value(record.get(field))
        )
        valuable_score = (valuable_filled / len(self.VALUABLE_FIELDS)) * 40

        return required_score + valuable_score

    def _calculate_accuracy(self, record: dict) -> float:
        """Calculate accuracy score based on validation results."""
        validation = record.get("_validation", {})

        if not validation:
            # No validation done - assume moderate accuracy
            return 60

        score = 60  # Base score

        # DNS validation
        if validation.get("dns_mx_valid") is True:
            score += 20
        elif validation.get("dns_mx_valid") is False:
            score -= 20

        # Google Places validation
        if validation.get("google_places_matched") is True:
            score += 15
        elif validation.get("google_places_matched") is False:
            score -= 10

        # LinkedIn validation
        if validation.get("linkedin_found") is True:
            score += 5

        # External validation score if present
        if "validation_score" in record:
            # Blend with validation score
            score = (score + record["validation_score"]) / 2

        return max(0, min(100, score))

    def _calculate_freshness(self, record: dict) -> float:
        """Calculate freshness score based on extraction date."""
        extracted_at = record.get("extracted_at")

        if not extracted_at:
            return 50  # Unknown freshness

        try:
            # Parse ISO datetime
            if isinstance(extracted_at, str):
                extracted_dt = datetime.fromisoformat(extracted_at.replace("Z", "+00:00"))
            else:
                extracted_dt = extracted_at

            # Calculate days since extraction
            now = datetime.now(extracted_dt.tzinfo) if extracted_dt.tzinfo else datetime.now(UTC)
            days_old = (now - extracted_dt).days

            # Score based on age
            if days_old <= 7:
                return 100
            elif days_old <= 30:
                return 90
            elif days_old <= 90:
                return 75
            elif days_old <= 180:
                return 60
            elif days_old <= 365:
                return 40
            else:
                return 20

        except Exception:
            return 50

    def _calculate_source_reliability(self, record: dict) -> float:
        """Calculate source reliability score."""
        sources = []

        # Check firmographic source
        firmographic_source = record.get("firmographic_source")
        if firmographic_source:
            sources.append(self.SOURCE_SCORES.get(firmographic_source, 50))

        # Check tech source
        tech_source = record.get("tech_source")
        if tech_source:
            sources.append(self.SOURCE_SCORES.get(tech_source, 50))

        # Check contact sources
        contacts = record.get("contacts", [])
        for contact in contacts:
            contact_source = contact.get("source")
            if contact_source:
                sources.append(self.SOURCE_SCORES.get(contact_source, 50))

        # Base score from association data
        if record.get("association"):
            sources.append(self.SOURCE_SCORES.get("association", 60))

        if not sources:
            return 50  # Default for unknown sources

        # Return weighted average (more sources = higher confidence)
        base_score = sum(sources) / len(sources)

        # Bonus for multiple sources
        source_bonus = min(10, len(sources) * 2)

        return min(100, base_score + source_bonus)

    def _has_value(self, value) -> bool:
        """Check if a field has a meaningful value."""
        if value is None:
            return False
        if isinstance(value, str) and not value.strip():
            return False
        if isinstance(value, list) and len(value) == 0:
            return False
        return True

    def _get_grade(self, score: int) -> str:
        """Convert numeric score to letter grade."""
        if score >= 90:
            return "A"
        elif score >= 80:
            return "B"
        elif score >= 70:
            return "C"
        elif score >= 60:
            return "D"
        else:
            return "F"
