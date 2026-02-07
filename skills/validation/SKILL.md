# Validation Agents Skill

## Overview
Validation agents ensure data quality through deduplication, external verification, and quality scoring.

## Agents
1. **Duplicate Detection Agent**: Fuzzy matching to merge duplicate records
2. **Cross-Reference Agent**: Validates against Google Places, LinkedIn, DNS
3. **Quality Scorer Agent**: Assigns 0-100 quality scores

## Quality Scoring
- Completeness: 30% (fields populated / total fields)
- Accuracy: 40% (validated fields / validatable fields)  
- Freshness: 15% (days since verification)
- Source Reliability: 15% (source tier scores)

## Minimum Score: 60

See full documentation in the PRD.
