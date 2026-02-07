"""
CrossRef Agent
NAM Intelligence Pipeline

Validates company records against external sources.
"""

import asyncio
import os
import re
import socket
from datetime import datetime, UTC
from typing import Optional

from agents.base import BaseAgent
from skills.common.SKILL import extract_domain


class CrossRefAgent(BaseAgent):
    """
    Cross-Reference Agent - validates records against external sources.

    Responsibilities:
    - DNS MX record validation (verify email domain)
    - Google Places API verification
    - LinkedIn company page lookup
    - Flag mismatches and issues
    """

    def _setup(self, **kwargs):
        """Initialize cross-ref settings."""
        self.methods = self.agent_config.get("methods", ["dns_mx", "google_places"])
        self.skip_unverifiable = self.agent_config.get("skip_unverifiable", False)

    async def run(self, task: dict) -> dict:
        """
        Cross-reference and validate records.

        Args:
            task: {
                "records": [{...}, ...]
            }

        Returns:
            {
                "success": True,
                "records": [{...validated...}, ...],
                "validation_stats": {...}
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

        self.log.info(f"Cross-referencing {len(records)} records")

        validated_records = []
        stats = {
            "dns_valid": 0,
            "dns_invalid": 0,
            "dns_skipped": 0,
            "places_matched": 0,
            "places_unmatched": 0,
            "places_skipped": 0,
        }

        for i, record in enumerate(records):
            validation = {}
            domain = extract_domain(record.get("website", ""))

            # DNS MX validation
            if "dns_mx" in self.methods and domain:
                is_valid = await self._validate_dns_mx(domain)
                validation["dns_mx_valid"] = is_valid
                if is_valid:
                    stats["dns_valid"] += 1
                elif is_valid is False:
                    stats["dns_invalid"] += 1
                else:
                    stats["dns_skipped"] += 1
            else:
                stats["dns_skipped"] += 1

            # Google Places validation
            if "google_places" in self.methods:
                company_name = record.get("company_name", "")
                city = record.get("city", "")
                state = record.get("state", "")

                if company_name and (city or state):
                    places_match = await self._validate_google_places(
                        company_name, city, state
                    )
                    validation["google_places_matched"] = places_match
                    if places_match:
                        stats["places_matched"] += 1
                    elif places_match is False:
                        stats["places_unmatched"] += 1
                    else:
                        stats["places_skipped"] += 1
                else:
                    stats["places_skipped"] += 1

            # LinkedIn validation
            if "linkedin" in self.methods and domain:
                linkedin_found = await self._validate_linkedin(domain)
                validation["linkedin_found"] = linkedin_found

            # Add validation results to record
            record["_validation"] = validation
            record["validated_at"] = datetime.now(UTC).isoformat()

            # Calculate validation score
            validation_score = self._calculate_validation_score(validation)
            record["validation_score"] = validation_score

            # Flag issues
            issues = []
            if validation.get("dns_mx_valid") is False:
                issues.append("invalid_domain")
            if validation.get("google_places_matched") is False:
                issues.append("address_not_found")

            if issues:
                record["_issues"] = issues

            validated_records.append(record)

            if (i + 1) % 100 == 0:
                self.log.info(f"Validated {i + 1}/{len(records)} records")

        self.log.info(
            f"Cross-reference complete",
            dns_valid=stats["dns_valid"],
            places_matched=stats["places_matched"]
        )

        return {
            "success": True,
            "records": validated_records,
            "validation_stats": stats,
            "records_processed": len(records)
        }

    async def _validate_dns_mx(self, domain: str) -> Optional[bool]:
        """Validate domain has MX records (can receive email).

        Three-tier fallback:
        1. aiodns (native async, best)
        2. dnspython (threaded via asyncio.to_thread)
        3. socket (threaded via asyncio.to_thread, last resort)
        """
        # Tier 1: aiodns (native async)
        try:
            import aiodns
            resolver = aiodns.DNSResolver()
            try:
                mx_records = await resolver.query(domain, 'MX')
                return bool(mx_records)
            except aiodns.error.DNSError:
                # No MX — try A record
                try:
                    a_records = await resolver.query(domain, 'A')
                    return bool(a_records)
                except aiodns.error.DNSError:
                    return False
        except ImportError:
            pass
        except Exception as e:
            self.log.warning(
                "dns_mx_validation_failed",
                domain=domain,
                error=str(e),
                error_type=type(e).__name__,
            )
            return None

        # Tier 2: dnspython (threaded)
        try:
            import dns.resolver
            try:
                mx_records = await asyncio.to_thread(dns.resolver.resolve, domain, 'MX')
                return bool(mx_records)
            except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer):
                try:
                    a_records = await asyncio.to_thread(dns.resolver.resolve, domain, 'A')
                    return bool(a_records)
                except Exception as e:
                    self.log.debug(
                        "dns_a_fallback_failed",
                        domain=domain,
                        error=str(e),
                        error_type=type(e).__name__,
                    )
                    return False
        except ImportError:
            pass
        except Exception as e:
            self.log.warning(
                "dns_mx_validation_failed",
                domain=domain,
                error=str(e),
                error_type=type(e).__name__,
            )
            return None

        # Tier 3: socket (threaded — never block the event loop)
        try:
            await asyncio.to_thread(socket.gethostbyname, domain)
            return True
        except socket.gaierror:
            return False

    async def _validate_google_places(
        self,
        company_name: str,
        city: str,
        state: str
    ) -> Optional[bool]:
        """Validate company exists at location using Google Places."""
        api_key = os.getenv("GOOGLE_PLACES_API_KEY")
        if not api_key:
            return None

        try:
            query = f"{company_name} {city} {state}"

            response = await self.http.get(
                "https://maps.googleapis.com/maps/api/place/textsearch/json",
                params={
                    "query": query,
                    "key": api_key,
                    "type": "establishment"
                }
            )

            if response.status_code == 200:
                data = response.json()
                results = data.get("results", [])

                if not results:
                    return False

                # Check if any result matches company name
                company_lower = company_name.lower()
                for result in results[:5]:
                    result_name = result.get("name", "").lower()
                    # Check for significant name overlap
                    if self._names_match(company_lower, result_name):
                        return True

                return False

        except Exception as e:
            self.log.warning(
                "google_places_validation_failed",
                provider="google_places",
                company_name=company_name,
                error=str(e),
                error_type=type(e).__name__,
            )

        return None

    async def _validate_linkedin(self, domain: str) -> Optional[bool]:
        """Check if company has LinkedIn page."""
        # This would typically use LinkedIn API
        # For now, we'll do a simple check
        try:
            response = await self.http.get(
                f"https://www.linkedin.com/company/{domain.replace('.', '-')}",
                timeout=10,
                retries=1
            )

            return response.status_code == 200

        except Exception as e:
            self.log.debug(
                "linkedin_validation_failed",
                provider="linkedin",
                domain=domain,
                error=str(e),
                error_type=type(e).__name__,
            )
            return None

    def _names_match(self, name1: str, name2: str) -> bool:
        """Check if two company names match."""
        # Normalize names
        def normalize(name):
            # Remove common suffixes
            name = re.sub(r'\b(inc|llc|ltd|corp|co|company)\b\.?', '', name)
            # Remove punctuation
            name = re.sub(r'[^\w\s]', '', name)
            # Normalize whitespace
            return ' '.join(name.split())

        n1 = normalize(name1)
        n2 = normalize(name2)

        if n1 == n2:
            return True

        # Check if one contains the other
        if n1 in n2 or n2 in n1:
            return True

        # Check word overlap
        words1 = set(n1.split())
        words2 = set(n2.split())

        if not words1 or not words2:
            return False

        overlap = len(words1 & words2)
        min_words = min(len(words1), len(words2))

        return overlap / min_words >= 0.5

    def _calculate_validation_score(self, validation: dict) -> int:
        """Calculate validation score from validation results."""
        score = 50  # Base score

        if validation.get("dns_mx_valid") is True:
            score += 20
        elif validation.get("dns_mx_valid") is False:
            score -= 20

        if validation.get("google_places_matched") is True:
            score += 20
        elif validation.get("google_places_matched") is False:
            score -= 10

        if validation.get("linkedin_found") is True:
            score += 10

        return max(0, min(100, score))
