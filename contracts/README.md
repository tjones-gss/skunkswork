# Agent Contracts

This directory contains JSON Schema contracts that define strict input/output interfaces for all pipeline agents.

## Purpose

Contracts ensure:
1. **Deterministic Pipeline** - Every agent has well-defined inputs and outputs
2. **Validation** - Runtime validation catches malformed data early
3. **Documentation** - Contracts serve as living API documentation
4. **Testing** - Contracts enable automated contract testing

## Directory Structure

```
contracts/
├── README.md                    # This file
├── validator.py                 # Contract validation utility
└── schemas/
    ├── core/                    # Core entity schemas
    │   ├── company.json
    │   ├── event.json
    │   ├── participant.json
    │   ├── competitor_signal.json
    │   ├── provenance.json
    │   ├── access_verdict.json
    │   └── page_classification.json
    ├── discovery/               # Discovery agent contracts
    │   ├── access_gatekeeper_input.json
    │   ├── access_gatekeeper_output.json
    │   ├── site_mapper_input.json
    │   ├── site_mapper_output.json
    │   ├── link_crawler_input.json
    │   ├── link_crawler_output.json
    │   ├── page_classifier_input.json
    │   └── page_classifier_output.json
    ├── extraction/              # Extraction agent contracts
    │   ├── html_parser_input.json
    │   ├── html_parser_output.json
    │   ├── event_extractor_input.json
    │   ├── event_extractor_output.json
    │   ├── event_participant_extractor_input.json
    │   ├── event_participant_extractor_output.json
    │   ├── api_client_input.json
    │   ├── api_client_output.json
    │   ├── pdf_parser_input.json
    │   └── pdf_parser_output.json
    ├── enrichment/              # Enrichment agent contracts
    │   ├── firmographic_input.json
    │   ├── firmographic_output.json
    │   ├── tech_stack_input.json
    │   ├── tech_stack_output.json
    │   ├── contact_finder_input.json
    │   └── contact_finder_output.json
    ├── validation/              # Validation agent contracts
    │   ├── dedupe_input.json
    │   ├── dedupe_output.json
    │   ├── crossref_input.json
    │   ├── crossref_output.json
    │   ├── scorer_input.json
    │   ├── scorer_output.json
    │   ├── entity_resolver_input.json
    │   └── entity_resolver_output.json
    ├── intelligence/            # Intelligence agent contracts
    │   ├── competitor_signal_miner_input.json
    │   ├── competitor_signal_miner_output.json
    │   ├── relationship_graph_builder_input.json
    │   └── relationship_graph_builder_output.json
    ├── export/                  # Export agent contracts
    │   ├── export_activation_input.json
    │   └── export_activation_output.json
    └── monitoring/              # Monitoring agent contracts
        ├── source_monitor_input.json
        └── source_monitor_output.json
```

## Schema Conventions

### Naming
- `{agent_name}_input.json` - Input contract for agent
- `{agent_name}_output.json` - Output contract for agent
- Core schemas are named after the entity type

### Schema Structure
All schemas follow JSON Schema Draft 2020-12:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://nam-pipeline/contracts/schemas/{category}/{name}.json",
  "title": "Human-readable title",
  "description": "What this schema validates",
  "type": "object",
  "properties": { ... },
  "required": [ ... ],
  "additionalProperties": false
}
```

### References
Core schemas are referenced in agent contracts via `$ref`:

```json
{
  "properties": {
    "companies": {
      "type": "array",
      "items": { "$ref": "../core/company.json" }
    }
  }
}
```

## Usage

### Validation Decorator

```python
from contracts.validator import validate_contract

class MyAgent(BaseAgent):
    @validate_contract
    async def run(self, task: dict) -> dict:
        # task is validated against {agent_type}_input.json
        # return value is validated against {agent_type}_output.json
        return result
```

### Manual Validation

```python
from contracts.validator import ContractValidator

validator = ContractValidator()

# Validate single record
is_valid, errors = validator.validate("core/company", record)

# Validate agent input
is_valid, errors = validator.validate_input("extraction.html_parser", task)

# Validate agent output
is_valid, errors = validator.validate_output("extraction.html_parser", result)
```

### PolicyChecker Integration

```python
from contracts.validator import ContractPolicy

# Used by middleware/policy.py
policy = ContractPolicy()
policy.enforce(agent_type, direction="input", data=task)
```

## Adding New Contracts

1. Create the schema file in the appropriate category folder
2. Reference core schemas where applicable
3. Add validation tests in `tests/test_contracts.py`
4. Update this README if adding a new category

## Validation Rules

### Required Fields
All schemas specify `required` fields that must be present:
- `company_name` for Company
- `url` for PageClassification
- `success` for all agent outputs

### Type Constraints
- Dates: ISO 8601 format strings
- URLs: Must be valid URI format
- Enums: Restricted to defined values
- Confidence scores: Number between 0.0 and 1.0

### Provenance
All extracted records must include provenance:
```json
{
  "provenance": [{
    "source_url": "https://...",
    "extracted_at": "2024-01-01T00:00:00Z",
    "extracted_by": "agent_type"
  }]
}
```

## Testing

```bash
# Run contract validation tests
pytest tests/test_contracts.py

# Validate a specific schema
python -m contracts.validator --schema core/company --file data/sample.json
```
