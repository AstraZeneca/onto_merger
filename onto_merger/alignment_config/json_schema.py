schema = {
    "name": "parser_config",
    "type": "object",
    "required": [
        "domain_node_type",
        "seed_ontology_name",
        "required_full_hierarchies",
        "mappings",
    ],
    "additionalProperties": False,
    "properties": {
        "domain_node_type": {"type": "string"},
        "seed_ontology_name": {"type": "string"},
        "required_full_hierarchies": {
            "type": "array",
            "minItems": 1,
            "items": {"type": "string"},
        },
        "mappings": {
            "type": "object",
            "required": ["type_groups", "directionality"],
            "additionalProperties": False,
            "properties": {
                "type_groups": {
                    "type": "object",
                    "required": ["equivalence", "database_reference", "label_match"],
                    "additionalProperties": False,
                    "properties": {
                        "equivalence": {
                            "type": "array",
                            "minItems": 1,
                            "items": {"type": "string"},
                        },
                        "database_reference": {
                            "type": "array",
                            "minItems": 0,
                            "items": {"type": "string"},
                        },
                        "label_match": {
                            "type": "array",
                            "minItems": 0,
                            "items": {"type": "string"},
                        },
                    },
                },
                "directionality": {
                    "type": "object",
                    "required": ["uni", "symmetric"],
                    "additionalProperties": False,
                    "properties": {
                        "symmetric": {
                            "type": "array",
                            "minItems": 1,
                            "items": {"type": "string"},
                        },
                        "uni": {
                            "type": "array",
                            "minItems": 0,
                            "items": {"type": "string"},
                        },
                    },
                },
            },
        },
    },
}
