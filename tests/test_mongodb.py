import pytest
from fatcat_utils.mongodb.database_handler import DatabaseHandler


@pytest.fixture
def database() -> DatabaseHandler:
    return DatabaseHandler()


@pytest.mark.asyncio
async def test_setup_mongo(database: DatabaseHandler):
    await database.setup()

@pytest.mark.asyncio
async def test_queue_creation(database: DatabaseHandler):
    assert await database.create_queue(
        queue={
            "queue_name": "test",
            "tags": ["a", "b"]
        },
        schemas=[{
            "stringtest": {
                "type": "string",
                "match": r"\d+"
            },
            "integertest": {
                "type": "integer",
                "lt": 6,
                "gt": 4
            }
        }]
    ) is not None

@pytest.mark.asyncio
async def test_getting_queue(database: DatabaseHandler):
    result = await database.get_queue({"FATCAT_QUEUE_TAG": "a"})
    assert result is not None and not result == {}


@pytest.mark.asyncio
async def test_getting_queue_based_on_name(database: DatabaseHandler):
    result = await database.get_queue_based_on_name("test")
    assert result is not None and not result == {}


@pytest.mark.asyncio
async def test_update_queue(database: DatabaseHandler):
    await database.update_queue({
        "queue_name": "test",
        "tags": ["a", "b", "c", "d"],
        "message_schemas_ids": []
    })
