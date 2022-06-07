"""JSON schema definition for the alignment and connectivity process configuration file."""

schema = {
    "name": "parser_config",
    "type": "object",
    "required": ["domain_node_type", "seed_ontology_name", "mappings"],
    "additionalProperties": True,
    "properties": {
        "domain_node_type": {"type": "string"},
        "seed_ontology_name": {"type": "string"},
        "force_through_failed_validation": {"type": "bool"},
        "image_format": {"type": "string", "pattern": "^(png|svg|html)$"},
        "mappings": {
            "type": "object",
            "required": ["type_groups"],
            "additionalProperties": False,
            "properties": {
                "type_groups": {
                    "type": "object",
                    "required": ["equivalence", "database_reference", "label_match"],
                    "additionalProperties": False,
                    "properties": {
                        "equivalence": {"type": "array", "minItems": 1, "items": {"type": "string"}},
                        "database_reference": {"type": "array", "minItems": 0, "items": {"type": "string"}},
                        "label_match": {"type": "array", "minItems": 0, "items": {"type": "string"}},
                    },
                }
            },
        },
    },
}
