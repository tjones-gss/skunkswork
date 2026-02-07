"""
Competitor Signal Miner Agent
NAM Intelligence Pipeline

Scans web pages for mentions of competitor ERP/manufacturing
software brands to build competitive intelligence.
"""

import re
from datetime import datetime, UTC
from typing import Optional

from bs4 import BeautifulSoup

from agents.base import BaseAgent
from models.ontology import (
    CompetitorSignal,
    CompetitorSignalType,
    Provenance,
    TARGET_COMPETITORS,
    COMPETITOR_ALIASES,
)
from middleware.policy import validate_json_output


class CompetitorSignalMinerAgent(BaseAgent):
    """
    Competitor Signal Miner Agent - detects competitor mentions.

    Responsibilities:
    - Scan pages for competitor brand mentions
    - Classify signal types (sponsor, exhibitor, member usage, etc.)
    - Extract context snippets
    - Track competitive intelligence
    """

    # Signal type indicators
    SIGNAL_TYPE_PATTERNS = {
        CompetitorSignalType.SPONSOR: [
            r'sponsor(ed|ship)?',
            r'supported by',
            r'brought to you by',
            r'partnered with',
        ],
        CompetitorSignalType.EXHIBITOR: [
            r'exhibitor',
            r'booth',
            r'vendor',
            r'exhibit(ing)?',
        ],
        CompetitorSignalType.MEMBER_USAGE: [
            r'(we )?(use|using|implement|run|powered by)',
            r'(our )?(erp|system|software)',
            r'success(ful)? (implementation|deployment)',
        ],
        CompetitorSignalType.SPEAKER_BIO: [
            r'speaker',
            r'presenter',
            r'panelist',
            r'(bio|biography)',
        ],
        CompetitorSignalType.PARTNER_INTEGRATION: [
            r'(integration|integrate[ds]?)',
            r'partner(ship)?',
            r'certified',
            r'alliance',
        ],
        CompetitorSignalType.JOB_POSTING: [
            r'job',
            r'career',
            r'hiring',
            r'position',
            r'experience (with|in)',
        ],
        CompetitorSignalType.CASE_STUDY: [
            r'case study',
            r'success story',
            r'customer story',
            r'testimonial',
        ],
        CompetitorSignalType.PRESS_RELEASE: [
            r'press release',
            r'announce',
            r'news',
        ],
    }

    def _setup(self, **kwargs):
        """Initialize miner settings."""
        self.max_signals = self.agent_config.get("max_signals", 100)
        self.context_window = self.agent_config.get("context_window", 150)

        # Build regex patterns for competitors
        self._build_competitor_patterns()

    def _build_competitor_patterns(self):
        """Build regex patterns for competitor matching."""
        self.competitor_patterns = {}

        for competitor, aliases in COMPETITOR_ALIASES.items():
            # Create pattern that matches any alias
            escaped_aliases = [re.escape(alias) for alias in aliases]
            pattern = r'\b(' + '|'.join(escaped_aliases) + r')\b'
            self.competitor_patterns[competitor] = re.compile(pattern, re.IGNORECASE)

    @validate_json_output
    async def run(self, task: dict) -> dict:
        """
        Scan content for competitor mentions.

        Args:
            task: {
                "url": "https://...",
                "html": "<html>...",  # Optional: pre-fetched content
                "text": "plain text",  # Optional: plain text to scan
                "source_company_id": "uuid",  # Optional: link to company
                "source_event_id": "uuid",  # Optional: link to event
                "association": "PMA"
            }

        Returns:
            {
                "success": True,
                "signals": [CompetitorSignal, ...],
                "competitor_summary": {"Epicor": 3, "SAP": 1},
                "records_processed": 1
            }
        """
        url = task.get("url")
        html = task.get("html")
        text = task.get("text")
        source_company_id = task.get("source_company_id")
        source_event_id = task.get("source_event_id")
        association = task.get("association")

        if not url and not html and not text:
            return {
                "success": False,
                "error": "URL, HTML, or text is required",
                "records": [],
                "records_processed": 0
            }

        # Fetch page if needed
        if not html and not text and url:
            try:
                response = await self.http.get(url, timeout=30)
                if response.status_code == 200:
                    html = response.text
            except Exception as e:
                self.log.warning(f"Failed to fetch {url}: {e}")

        # Extract text from HTML
        if html and not text:
            soup = BeautifulSoup(html, "lxml")
            text = soup.get_text(separator=" ")

        if not text:
            return {
                "success": False,
                "error": "No content to scan",
                "records": [],
                "records_processed": 0
            }

        # Create provenance
        provenance = Provenance(
            source_url=url or "text_input",
            source_type="web" if url else "text",
            extracted_by=self.agent_type,
            association_code=association,
            job_id=self.job_id
        )

        # Mine for competitor signals
        signals = self._mine_signals(
            text,
            url,
            source_company_id,
            source_event_id,
            association,
            provenance
        )

        # Build summary
        competitor_summary = {}
        for signal in signals:
            comp = signal.competitor_name
            competitor_summary[comp] = competitor_summary.get(comp, 0) + 1

        self.log.info(
            f"Found {len(signals)} competitor signals",
            competitors=list(competitor_summary.keys()),
            url=url
        )

        # Convert to dicts
        signal_dicts = [s.model_dump() if hasattr(s, 'model_dump') else s for s in signals]

        return {
            "success": True,
            "records": signal_dicts,
            "signals": signal_dicts,
            "competitor_summary": competitor_summary,
            "records_processed": 1
        }

    def _mine_signals(
        self,
        text: str,
        url: Optional[str],
        source_company_id: Optional[str],
        source_event_id: Optional[str],
        association: Optional[str],
        provenance: Provenance
    ) -> list[CompetitorSignal]:
        """Mine text for competitor signals."""
        signals = []
        text_lower = text.lower()

        for competitor, pattern in self.competitor_patterns.items():
            matches = list(pattern.finditer(text))

            for match in matches:
                # Extract context
                start = max(0, match.start() - self.context_window)
                end = min(len(text), match.end() + self.context_window)
                context = text[start:end].strip()

                # Clean up context
                context = re.sub(r'\s+', ' ', context)
                if start > 0:
                    context = "..." + context
                if end < len(text):
                    context = context + "..."

                # Determine signal type
                signal_type = self._classify_signal_type(context, text_lower)

                # Calculate confidence
                confidence = self._calculate_confidence(match.group(), context)

                signal = CompetitorSignal(
                    competitor_name=competitor.title(),
                    competitor_normalized=competitor.lower(),
                    signal_type=signal_type,
                    context=context,
                    confidence=confidence,
                    source_company_id=source_company_id,
                    source_event_id=source_event_id,
                    source_association=association,
                    provenance=[provenance]
                )

                signals.append(signal)

                if len(signals) >= self.max_signals:
                    break

            if len(signals) >= self.max_signals:
                break

        return signals

    def _classify_signal_type(self, context: str, full_text: str) -> CompetitorSignalType:
        """Classify the type of competitor signal."""
        context_lower = context.lower()

        # Score each signal type
        scores = {}

        for signal_type, patterns in self.SIGNAL_TYPE_PATTERNS.items():
            score = 0
            for pattern in patterns:
                if re.search(pattern, context_lower):
                    score += 2  # Context match is strong
                elif re.search(pattern, full_text[:2000]):
                    score += 1  # Page context match is weaker

            if score > 0:
                scores[signal_type] = score

        # Return highest scoring type
        if scores:
            return max(scores, key=scores.get)

        # Default based on context clues
        return CompetitorSignalType.MEMBER_USAGE

    def _calculate_confidence(self, matched_text: str, context: str) -> float:
        """Calculate confidence score for the signal."""
        confidence = 0.7  # Base confidence

        # Boost for exact product name match
        if matched_text.lower() in [alias for aliases in COMPETITOR_ALIASES.values() for alias in aliases]:
            confidence += 0.1

        # Boost for specific context
        specific_indicators = ['erp', 'software', 'system', 'implementation', 'using']
        for indicator in specific_indicators:
            if indicator in context.lower():
                confidence += 0.05

        # Cap at 0.95
        return min(confidence, 0.95)

    async def scan_batch(self, task: dict) -> dict:
        """
        Scan multiple pages for competitor mentions.

        Args:
            task: {
                "pages": [
                    {"url": "...", "html": "..."},
                    ...
                ]
            }
        """
        pages = task.get("pages", [])

        if not pages:
            return {
                "success": False,
                "error": "No pages provided",
                "records": [],
                "records_processed": 0
            }

        self.log.info(f"Scanning {len(pages)} pages for competitor signals")

        all_signals = []
        competitor_totals = {}

        for page in pages:
            result = await self.run(page)

            if result.get("success"):
                all_signals.extend(result.get("signals", []))

                for comp, count in result.get("competitor_summary", {}).items():
                    competitor_totals[comp] = competitor_totals.get(comp, 0) + count

        self.log.info(
            f"Batch scan complete",
            pages_scanned=len(pages),
            total_signals=len(all_signals),
            competitors_found=list(competitor_totals.keys())
        )

        return {
            "success": True,
            "records": all_signals,
            "signals": all_signals,
            "competitor_summary": competitor_totals,
            "pages_scanned": len(pages),
            "records_processed": len(pages)
        }


class CompetitorReportGenerator:
    """
    Generates competitor intelligence reports from signals.
    """

    @staticmethod
    def generate_report(signals: list[dict]) -> dict:
        """Generate a summary report from signals."""
        if not signals:
            return {"total_signals": 0}

        # Group by competitor
        by_competitor = {}
        for signal in signals:
            comp = signal.get("competitor_normalized", "unknown")
            if comp not in by_competitor:
                by_competitor[comp] = {
                    "name": signal.get("competitor_name"),
                    "signals": [],
                    "signal_types": {},
                    "associations": set(),
                    "events": set(),
                    "companies": set(),
                }

            by_competitor[comp]["signals"].append(signal)

            # Count signal types
            sig_type = signal.get("signal_type", "unknown")
            by_competitor[comp]["signal_types"][sig_type] = \
                by_competitor[comp]["signal_types"].get(sig_type, 0) + 1

            # Track sources
            if signal.get("source_association"):
                by_competitor[comp]["associations"].add(signal["source_association"])
            if signal.get("source_event_id"):
                by_competitor[comp]["events"].add(signal["source_event_id"])
            if signal.get("source_company_id"):
                by_competitor[comp]["companies"].add(signal["source_company_id"])

        # Build report
        report = {
            "total_signals": len(signals),
            "competitors": {},
            "generated_at": datetime.now(UTC).isoformat()
        }

        for comp, data in by_competitor.items():
            report["competitors"][comp] = {
                "name": data["name"],
                "total_signals": len(data["signals"]),
                "signal_types": data["signal_types"],
                "associations_present": list(data["associations"]),
                "events_present": len(data["events"]),
                "companies_using": len(data["companies"]),
            }

        # Sort by signal count
        report["competitors"] = dict(
            sorted(
                report["competitors"].items(),
                key=lambda x: x[1]["total_signals"],
                reverse=True
            )
        )

        return report
