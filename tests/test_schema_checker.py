import pytest
from fatcat_utils.schema_validator import match_to_schema


@pytest.mark.asyncio
async def test_schema_checker():
    body = {
        "a": 1,
        "b": 2,
        "c": "asdgdf",
        "d": "5",
        "e": "True",
        "f": "a",
        "g": -1.675,
        "h": {
            "te": "st",
            "test": "ing"
        },
        "i": [1, 2, 3, 4]
    }
    
    assert await match_to_schema(body, {
        "a": {
            "type": "integer",
            "lt": 4,
            "gte": 1
        },
        "b": {
            "type": "character"
        },
        "c": {
            "type": "string",
            "match": r".+"
        },
        "d": {
            "type": "integer"
        },
        "e": {
            "type": "boolean"
        },
        "f": {
            "type": "character"
        },
        "g": {
            "type": "float"
        },
        "h": {
            "type": "dictionary",
            "inner_schema": {
                "te": {
                    "type": "string",
                    "match": r"[a-zA-Z]+"
                },
                "test": {
                    "type": "string"
                }
            }
        },
        "i": {
            "type": "integer[]"
        }
    })
