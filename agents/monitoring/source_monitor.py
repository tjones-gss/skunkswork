"""
Source Monitor Agent
NAM Intelligence Pipeline

Monitors data sources for changes, DOM drift, and extraction failures.
"""

import hashlib
import json
import re
from datetime import datetime, UTC
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup

from agents.base import BaseAgent
from models.ontology import SourceBaseline
from middleware.policy import crawler_only, validate_json_output


class SourceMonitorAgent(BaseAgent):
    """
    Source Monitor Agent - detects changes in data sources.

    Responsibilities:
    - DOM drift detection
    - Selector change tracking
    - Rate limiting/blocking detection
    - Generate change reports with alert levels
    """

    # Alert levels
    ALERT_CRITICAL = "CRITICAL"
    ALERT_WARNING = "WARNING"
    ALERT_INFO = "INFO"

    def _setup(self, **kwargs):
        """Initialize monitor settings."""
        self.baseline_dir = Path(self.agent_config.get(
            "baseline_dir", "data/monitoring/baselines"
        ))
        self.baseline_dir.mkdir(parents=True, exist_ok=True)

        self.report_dir = Path(self.agent_config.get(
            "report_dir", "data/monitoring/reports"
        ))
        self.report_dir.mkdir(parents=True, exist_ok=True)

        self.drift_threshold = self.agent_config.get("drift_threshold", 0.2)

    @validate_json_output
    async def run(self, task: dict) -> dict:
        """
        Monitor sources for changes.

        Args:
            task: {
                "action": "check" | "baseline" | "report",
                "urls": ["https://...", ...],
                "selectors": {"company_name": ".member-name", ...},  # Optional
            }

        Returns:
            {
                "success": True,
                "changes_detected": True/False,
                "alerts": [{...}, ...],
                "report_path": "..."
            }
        """
        action = task.get("action", "check")

        if action == "check":
            return await self._check_sources(task)
        elif action == "baseline":
            return await self._create_baselines(task)
        elif action == "report":
            return await self._generate_report(task)
        else:
            return {
                "success": False,
                "error": f"Unknown action: {action}",
                "records_processed": 0
            }

    @crawler_only
    async def _check_sources(self, task: dict) -> dict:
        """Check sources for changes against baselines."""
        urls = task.get("urls", [])
        selectors = task.get("selectors", {})

        if not urls:
            return {
                "success": False,
                "error": "No URLs provided",
                "records_processed": 0
            }

        self.log.info(f"Checking {len(urls)} sources for changes")

        alerts = []
        changes_detected = False

        for url in urls:
            # Load baseline
            baseline = self._load_baseline(url)

            if not baseline:
                alerts.append({
                    "level": self.ALERT_INFO,
                    "url": url,
                    "message": "No baseline exists, creating one",
                    "action": "create_baseline"
                })
                # Create baseline
                await self._create_baseline_for_url(url, selectors)
                continue

            # Fetch current page
            try:
                response = await self.http.get(url, timeout=30)

                if response.status_code != 200:
                    alerts.append({
                        "level": self.ALERT_CRITICAL,
                        "url": url,
                        "message": f"Source returned HTTP {response.status_code}",
                        "status_code": response.status_code,
                        "action": "investigate"
                    })
                    changes_detected = True
                    continue

                html = response.text

            except Exception as e:
                alerts.append({
                    "level": self.ALERT_CRITICAL,
                    "url": url,
                    "message": f"Failed to fetch source: {e}",
                    "error": str(e),
                    "action": "investigate"
                })
                changes_detected = True
                continue

            # Check for changes
            change_alerts = self._compare_to_baseline(url, html, baseline, selectors)

            if change_alerts:
                changes_detected = True
                alerts.extend(change_alerts)

                # Update baseline
                self._update_baseline(baseline, html, len(change_alerts))

        # Generate report if changes detected
        report_path = None
        if changes_detected:
            report_path = self._save_alerts_report(alerts)

        self.log.info(
            f"Source check complete",
            urls_checked=len(urls),
            changes_detected=changes_detected,
            alerts=len(alerts)
        )

        return {
            "success": True,
            "changes_detected": changes_detected,
            "alerts": alerts,
            "report_path": report_path,
            "records_processed": len(urls)
        }

    async def _create_baselines(self, task: dict) -> dict:
        """Create baselines for URLs."""
        urls = task.get("urls", [])
        selectors = task.get("selectors", {})

        if not urls:
            return {
                "success": False,
                "error": "No URLs provided",
                "records_processed": 0
            }

        self.log.info(f"Creating baselines for {len(urls)} URLs")

        created = 0
        errors = []

        for url in urls:
            try:
                await self._create_baseline_for_url(url, selectors)
                created += 1
            except Exception as e:
                errors.append({"url": url, "error": str(e)})

        return {
            "success": True,
            "baselines_created": created,
            "errors": errors,
            "records_processed": len(urls)
        }

    @crawler_only
    async def _create_baseline_for_url(
        self,
        url: str,
        selectors: dict = None
    ) -> SourceBaseline:
        """Create a baseline for a single URL."""
        response = await self.http.get(url, timeout=30)

        if response.status_code != 200:
            raise Exception(f"HTTP {response.status_code}")

        html = response.text
        soup = BeautifulSoup(html, "lxml")

        # Calculate hashes
        url_hash = self._hash_string(url)
        content_hash = self._hash_string(html)
        structure_hash = self._hash_structure(soup)

        # Hash selectors if provided
        selector_hashes = {}
        if selectors:
            for name, selector in selectors.items():
                elements = soup.select(selector)
                selector_hashes[name] = self._hash_string(str(elements))

        # Count expected items
        expected_count = self._count_items(soup)

        baseline = SourceBaseline(
            url=url,
            url_hash=url_hash,
            domain=self._extract_domain(url),
            selector_hashes=selector_hashes,
            page_structure_hash=structure_hash,
            expected_item_count=expected_count,
            content_hash=content_hash,
            last_checked_at=datetime.now(UTC)
        )

        # Save baseline
        self._save_baseline(baseline)

        self.log.debug(f"Created baseline for {url}")

        return baseline

    def _compare_to_baseline(
        self,
        url: str,
        html: str,
        baseline: SourceBaseline,
        selectors: dict = None
    ) -> list[dict]:
        """Compare current page to baseline."""
        alerts = []
        soup = BeautifulSoup(html, "lxml")

        # Check structure hash
        current_structure = self._hash_structure(soup)
        if current_structure != baseline.page_structure_hash:
            drift = self._calculate_drift(soup, baseline)

            if drift > self.drift_threshold:
                alerts.append({
                    "level": self.ALERT_CRITICAL,
                    "url": url,
                    "type": "DOM_DRIFT",
                    "message": f"Page structure changed significantly ({drift:.1%} drift)",
                    "drift_percentage": drift,
                    "action": "update_selectors"
                })
            else:
                alerts.append({
                    "level": self.ALERT_WARNING,
                    "url": url,
                    "type": "DOM_DRIFT",
                    "message": f"Minor page structure change ({drift:.1%} drift)",
                    "drift_percentage": drift,
                    "action": "monitor"
                })

        # Check selector hashes
        if selectors and baseline.selector_hashes:
            for name, selector in selectors.items():
                elements = soup.select(selector)
                current_hash = self._hash_string(str(elements))
                baseline_hash = baseline.selector_hashes.get(name)

                if baseline_hash and current_hash != baseline_hash:
                    if not elements:
                        alerts.append({
                            "level": self.ALERT_CRITICAL,
                            "url": url,
                            "type": "SELECTOR_BROKEN",
                            "message": f"Selector '{name}' ({selector}) returns no results",
                            "selector": selector,
                            "action": "fix_selector"
                        })
                    else:
                        alerts.append({
                            "level": self.ALERT_WARNING,
                            "url": url,
                            "type": "SELECTOR_CHANGED",
                            "message": f"Selector '{name}' results changed",
                            "selector": selector,
                            "action": "verify_extraction"
                        })

        # Check item count
        current_count = self._count_items(soup)
        if baseline.expected_item_count:
            count_diff = abs(current_count - baseline.expected_item_count)
            pct_diff = count_diff / baseline.expected_item_count if baseline.expected_item_count > 0 else 0

            if current_count == 0 and baseline.expected_item_count > 0:
                alerts.append({
                    "level": self.ALERT_CRITICAL,
                    "url": url,
                    "type": "ITEMS_MISSING",
                    "message": f"Expected {baseline.expected_item_count} items, found 0",
                    "expected": baseline.expected_item_count,
                    "actual": current_count,
                    "action": "investigate"
                })
            elif pct_diff > 0.5:
                alerts.append({
                    "level": self.ALERT_WARNING,
                    "url": url,
                    "type": "ITEM_COUNT_CHANGED",
                    "message": f"Item count changed significantly: {baseline.expected_item_count} -> {current_count}",
                    "expected": baseline.expected_item_count,
                    "actual": current_count,
                    "action": "verify"
                })

        # Check for blocking indicators
        blocking_indicators = self._check_blocking(html)
        if blocking_indicators:
            alerts.append({
                "level": self.ALERT_CRITICAL,
                "url": url,
                "type": "ACCESS_BLOCKED",
                "message": f"Possible blocking detected: {blocking_indicators}",
                "indicators": blocking_indicators,
                "action": "review_access"
            })

        return alerts

    def _hash_string(self, s: str) -> str:
        """Create SHA-256 hash of string."""
        return hashlib.sha256(s.encode("utf-8")).hexdigest()

    def _hash_structure(self, soup: BeautifulSoup) -> str:
        """Create hash of page structure (tags without content)."""
        def get_structure(elem, depth=0):
            if depth > 5:  # Limit depth
                return ""

            if not hasattr(elem, "name") or elem.name is None:
                return ""

            children = "".join(
                get_structure(child, depth + 1)
                for child in elem.children
                if hasattr(child, "name")
            )

            classes = elem.get("class", [])
            class_str = ".".join(sorted(classes)) if classes else ""

            return f"<{elem.name} class='{class_str}'>{children}</{elem.name}>"

        structure = get_structure(soup.body or soup)
        return self._hash_string(structure)

    def _calculate_drift(self, soup: BeautifulSoup, baseline: SourceBaseline) -> float:
        """Calculate structural drift percentage."""
        # Compare tag counts
        current_tags = {}
        for tag in soup.find_all():
            current_tags[tag.name] = current_tags.get(tag.name, 0) + 1

        # This is a simplified drift calculation
        # In production, you'd want a more sophisticated comparison
        return 0.1  # Placeholder - implement proper diff algorithm

    def _count_items(self, soup: BeautifulSoup) -> int:
        """Count likely data items on page."""
        # Look for common list patterns
        counts = [
            len(soup.find_all(class_=re.compile(r'member|company|listing|item|card', re.I))),
            len(soup.find_all("tr")) - 1 if soup.find("table") else 0,
            len(soup.find_all("li", class_=True)),
        ]

        return max(counts) if counts else 0

    def _check_blocking(self, html: str) -> list[str]:
        """Check for access blocking indicators."""
        indicators = []
        html_lower = html.lower()

        blocking_patterns = [
            ("rate limited", "rate_limit"),
            ("too many requests", "rate_limit"),
            ("access denied", "access_denied"),
            ("forbidden", "forbidden"),
            ("captcha", "captcha"),
            ("please verify", "verification"),
            ("unusual traffic", "unusual_traffic"),
        ]

        for pattern, indicator in blocking_patterns:
            if pattern in html_lower:
                indicators.append(indicator)

        return indicators

    def _extract_domain(self, url: str) -> str:
        """Extract domain from URL."""
        from urllib.parse import urlparse
        parsed = urlparse(url)
        return parsed.netloc

    def _load_baseline(self, url: str) -> Optional[SourceBaseline]:
        """Load baseline for URL."""
        url_hash = self._hash_string(url)
        path = self.baseline_dir / f"{url_hash}.json"

        if not path.exists():
            return None

        with open(path) as f:
            data = json.load(f)

        return SourceBaseline(**data)

    def _save_baseline(self, baseline: SourceBaseline):
        """Save baseline to disk."""
        path = self.baseline_dir / f"{baseline.url_hash}.json"

        with open(path, "w") as f:
            json.dump(baseline.model_dump(), f, indent=2, default=str)

    def _update_baseline(
        self,
        baseline: SourceBaseline,
        html: str,
        changes: int
    ):
        """Update baseline with change tracking."""
        baseline.last_checked_at = datetime.now(UTC)
        baseline.last_changed_at = datetime.now(UTC)
        baseline.change_count += changes
        baseline.content_hash = self._hash_string(html)
        baseline.updated_at = datetime.now(UTC)

        self._save_baseline(baseline)

    def _save_alerts_report(self, alerts: list[dict]) -> str:
        """Save alerts to a report file."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = self.report_dir / f"change_report_{timestamp}.json"

        report = {
            "generated_at": datetime.now(UTC).isoformat(),
            "alert_count": len(alerts),
            "critical_count": sum(1 for a in alerts if a["level"] == self.ALERT_CRITICAL),
            "warning_count": sum(1 for a in alerts if a["level"] == self.ALERT_WARNING),
            "info_count": sum(1 for a in alerts if a["level"] == self.ALERT_INFO),
            "alerts": alerts
        }

        with open(report_path, "w") as f:
            json.dump(report, f, indent=2)

        return str(report_path)

    async def _generate_report(self, task: dict) -> dict:
        """Generate a monitoring status report."""
        # Load all baselines
        baselines = []
        for path in self.baseline_dir.glob("*.json"):
            with open(path) as f:
                baselines.append(json.load(f))

        # Load recent alerts
        recent_alerts = []
        for path in sorted(self.report_dir.glob("*.json"), reverse=True)[:5]:
            with open(path) as f:
                recent_alerts.append(json.load(f))

        report = {
            "generated_at": datetime.now(UTC).isoformat(),
            "total_sources": len(baselines),
            "sources_with_changes": sum(1 for b in baselines if b.get("change_count", 0) > 0),
            "baselines": [
                {
                    "url": b["url"],
                    "domain": b.get("domain"),
                    "last_checked": b.get("last_checked_at"),
                    "change_count": b.get("change_count", 0),
                }
                for b in baselines
            ],
            "recent_alerts": recent_alerts
        }

        # Save report
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = self.report_dir / f"status_report_{timestamp}.json"

        with open(report_path, "w") as f:
            json.dump(report, f, indent=2)

        return {
            "success": True,
            "report_path": str(report_path),
            "total_sources": len(baselines),
            "records_processed": len(baselines)
        }
