from enum import Enum
from typing import Dict, Any, List, Optional, Union
from pymongo.errors import CollectionInvalid, OperationFailure
import motor.motor_asyncio as motor
from bson import ObjectId
from fatcat_utils.logger import logger
from .interfaces import IQueue, IQueueAgg
from .exceptions import TooManyArguments, InvalidType, LifeIsASimulation, InvalidSchema
from .config import MONGO_CONFIG


class IdType(Enum):
    QueueID = "queue"
    SchemasIDs = "schemas"


class DatabaseHandler:
    """The MongoDB Handler
    """
    def __init__(self):
        logger.debug("Creating a new mongo connection")
        self._server = motor.AsyncIOMotorClient(
            f"mongodb://{MONGO_CONFIG['username']}:{MONGO_CONFIG['password']}@{MONGO_CONFIG['hostname']}:{MONGO_CONFIG['port']}" if MONGO_CONFIG["username"] is not None and MONGO_CONFIG["password"] is not None
            else f"mongodb://{MONGO_CONFIG['hostname']}:{MONGO_CONFIG['port']}"
        )

    async def setup(self):
        """Sets up the DB environment. Should be ran first
        * Creates the Database
        * Creates the Queues Collection with a primary key set to `queue_name`
        * Creates the Schemas Collection
        """
        logger.debug("Initializing Mongo")
        logger.debug("Getting the database")
        database = self._server.get_database(MONGO_CONFIG["database_name"])

        logger.debug("Getting the queues collection")
        queues_collection = database.get_collection(
            MONGO_CONFIG["queues_collection"])
        logger.debug(
            "Creating an index for the queues collection (if doesn't already exists)")
        await queues_collection.create_index("queue_name", unique=True)
        logger.debug("Successfully created an index for the queues collection")

        try:
            logger.debug("Creating the failed collection")
            await database.create_collection(MONGO_CONFIG["failed_collection"])
        except (CollectionInvalid, OperationFailure) as e:
            logger.debug("Failed collection probably already exists", exc_info=e)
        except Exception as e:
            logger.critical(
                "Failed to create the failed collection. Can't proceed.", exc_info=e)
            raise

        try:
            logger.debug("Creating the worker failed collection")
            await database.create_collection(MONGO_CONFIG["worker_failed_collection"])
        except (CollectionInvalid, OperationFailure) as e:
            logger.debug("Worker failed collection probably already exists", exc_info=e)
        except Exception as e:
            logger.critical(
                "Failed to create the worker failed collection. Can't proceed.", exc_info=e)
            raise

        try:
            logger.debug("Creating the schemas collection")
            await database.create_collection(MONGO_CONFIG["schemas_collection"])
        except (CollectionInvalid, OperationFailure) as e:
            logger.debug("Schemas collection probably already exists", exc_info=e)
        except:
            logger.critical(
                "Failed to create the schemas collection. Can't proceed.", exc_info=e)
            raise

    async def add_fail(self, message: Dict[Any, Any]) -> ObjectId:
        """Adds a failed message to the DB

        :param message: The failed message
        :type message: Dict[Any, Any]
        :return: The ID of the inserted document
        :rtype: ObjectId
        """
        logger.debug("Creating a new failed document")
        database = self._server.get_database(MONGO_CONFIG["database_name"])
        collection = database.get_collection(MONGO_CONFIG["failed_collection"])
        return (await collection.insert_one(message)).inserted_id

    async def add_worker_fail(self, message: Dict[Any, Any]) -> ObjectId:
        """Adds a failed message to the DB

        :param message: The failed message
        :type message: Dict[Any, Any]
        :return: The ID of the inserted document
        :rtype: ObjectId
        """
        logger.debug("Creating a new failed document")
        database = self._server.get_database(MONGO_CONFIG["database_name"])
        collection = database.get_collection(
            MONGO_CONFIG["worker_failed_collection"])
        return (await collection.insert_one(message)).inserted_id

    async def get_queue_based_on_name(self, name: str) -> IQueueAgg:
        """Gets a queue document based on its name

        :param name: The name of the queue
        :type name: str
        :return: The queue document
        :rtype: IQueueAgg
        """
        logger.debug("Looking for the queue")
        database = self._server.get_database(MONGO_CONFIG["database_name"])
        collection = database.get_collection(MONGO_CONFIG["queues_collection"])

        queue = await collection.aggregate([
            {
                "$match": {
                    "queue_name": name
                }
            },
            {
                "$unwind": {
                    "path": "$message_schemas_ids",
                    "preserveNullAndEmptyArrays": True
                }
            },
            {
                "$lookup": {
                    "from": MONGO_CONFIG["schemas_collection"],
                    "foreignField": "_id",
                    "localField": "message_schemas_ids",
                    "as": "schema"
                }
            },
            {
                "$unwind": {
                    "path": "$schema",
                    "preserveNullAndEmptyArrays": True
                }
            },
            {
                "$group": {
                    "_id": "$_id",
                    "tags": {
                        "$first": "$tags"
                    },
                    "queue_name": {
                        "$first": "$queue_name"
                    },
                    "schemas": {
                        "$push": "$schema"
                    }
                }
            }
        ]).to_list(1)

        if len(queue) == 0:
            logger.warning(
                f"Oh no! Couldn't find the queue! This is the name btw {name}")
            queue = None
        else:
            queue = queue[0]
            logger.debug(f"Found the queue. It's {queue['queue_name']}")

        return queue

    async def get_queue(self, message: Dict[Any, Any]) -> IQueueAgg:
        """Returns the appropriate queue according to the `tag` key in the message

        :param message: The body of the RabbitMQ message
        :type message: Dict[Any, Any]
        :return: The document
        :rtype: IQueue
        """
        logger.debug("Looking for the queue")
        database = self._server.get_database(MONGO_CONFIG["database_name"])
        collection = database.get_collection(MONGO_CONFIG["queues_collection"])

        queue = await collection.aggregate([
            {
                "$match": {
                    "tags": message["FATCAT_QUEUE_TAG"]
                }
            },
            {
                "$unwind": {
                    "path": "$message_schemas_ids",
                    "preserveNullAndEmptyArrays": True
                }
            },
            {
                "$lookup": {
                    "from": MONGO_CONFIG["schemas_collection"],
                    "foreignField": "_id",
                    "localField": "message_schemas_ids",
                    "as": "schema"
                }
            },
            {
                "$unwind": {
                    "path": "$schema",
                    "preserveNullAndEmptyArrays": True
                }
            },
            {
                "$group": {
                    "_id": "$_id",
                    "tags": {
                        "$first": "$tags"
                    },
                    "queue_name": {
                        "$first": "$queue_name"
                    },
                    "schemas": {
                        "$push": "$schema"
                    }
                }
            }
        ]).to_list(1)

        if len(queue) == 0:
            logger.warning(
                f"Oh no! There is no queue corresponding to the message! This is the tag btw {message['FATCAT_QUEUE_TAG']}")
            queue = None
        else:
            queue = queue[0]
            logger.debug(f"Found the queue. It's {queue['queue_name']}")

        return queue

    async def create_queue(self, queue: IQueue, schemas: Optional[List[Dict[Any, Any]]] = None) -> Dict[IdType, Union[ObjectId, List[ObjectId]]]:
        """Inserts a new queue and schemas and returns the document IDs

        :param queue: The queue document to create
        :type queue: IQueue
        :param schemas: The schemas documents to create, defaults to None
        :type schemas: List[Dict[Any, Any]], optional
        :raises InvalidType: Raises if there are `message_schemas_ids` set not to string/ObjectId
        :raises LifeIsASimulation: Raises if there is no schema with the id set in `queue['message_schemas_ids']` and there are no schemas provided
        :raises TooManyArguments: Raises if there are more `message_schemas_ids` provided than `schemas`
        :raises InvalidSchema: Raises if a schema doesn't match the schema (ironic.)
        :return: Returns the IDs of the documents inserted
        :rtype: Dict[IdType, Union[ObjectId, List[ObjectId]]]
        """
        logger.debug("Creating a new queue")
        if "message_schemas_ids" in queue and queue["message_schemas_ids"] is not None and schemas is None:
            logger.debug(
                "There are already preset ids for the schemas but no schemas. Let's look for existing ones")
            for i in range(len(queue["message_schemas_ids"])):
                if isinstance(queue["message_schemas_ids"][i], str):
                    queue["message_schemas_ids"][i] = ObjectId(
                        queue["message_schemas_ids"][i])

                if not isinstance(queue["message_schemas_ids"][i], ObjectId):
                    logger.error("Got a bad schema id")
                    raise InvalidType(
                        f"Dude what is this? No seriously. What is this? Why did you give me {type(queue['message_schemas_ids'][i])} as a schema_id?")

                if not await self.check_if_document_exists(queue["message_schemas_ids"][i], is_schema=True):
                    logger.error(
                        f"Schema with id {repr(queue['message_schemas_ids'][i])} doesn't exist")
                    raise LifeIsASimulation(
                        f"There is no schema with the id {repr(queue['message_schemas_ids'][i])}. Life is a lie. I don't know what's real anymore.")

            return {
                IdType.QueueID: await self._create_new_queue(queue),
                IdType.SchemasIDs: []
            }

        elif "message_schemas_ids" in queue and queue['message_schemas_ids'] is not None and schemas is not None:
            logger.debug(
                "There are already preset ids for the schemas and schemas. Let's look for existing ones")
            schema_index = 0

            for i in range(len(queue['message_schemas_ids'])):
                if isinstance(queue['message_schemas_ids'][i], str):
                    logger.info("Converting the schema id...")
                    queue['message_schemas_ids'][i] = ObjectId(
                        queue['message_schemas_ids'][i])

                if not isinstance(queue['message_schemas_ids'][i], ObjectId):
                    logger.error("Got a bad schema id")
                    raise InvalidType(
                        f"Dude what is this? No seriously. What is this? Why did you give me {type(queue['message_schemas_ids'][i])} as a schema_id?")

                if await self.check_if_document_exists(queue['message_schemas_ids'][i], is_schema=True):
                    logger.info(
                        f"Schema {repr(queue['message_schemas_ids'][i])} exists. Let's skip to the next one.")
                    continue

                if schema_index >= len(schemas):
                    logger.error(
                        "Too many schema ids were given. Sorry m8 I don't work like that.")
                    raise TooManyArguments(
                        "Oi bruv you shouldn't put too many schema ids in ya list bro")

                if not self._is_schema_valid(schemas[schema_index]):
                    logger.error(f"Schema {schema_index} is invalid.")
                    raise InvalidSchema(
                        f"The schema that was provided is invalid. Index: {schema_index}")

                schemas[schema_index]["_id"] = queue['message_schemas_ids'][i]
                schema_index += 1

            return {
                IdType.SchemasIDs: await self._create_new_schemas(schemas),
                IdType.QueueID: await self._create_new_queue(queue)
            }

        elif ("message_schemas_ids" not in queue or queue['message_schemas_ids'] is None or len(queue['message_schemas_ids']) == 0) and schemas is not None:
            logger.debug("Schema ids weren't given, but schemas were given")

            for i in range(len(schemas)):
                if not self._is_schema_valid(schemas[i]):
                    logger.error(f"Schema {i} is invalid.")
                    raise InvalidSchema(
                        f"The schema that was provided is invalid. Index: {i}")

            logger.info("Creating new schemas")
            results = await self._create_new_schemas(schemas)
            queue['message_schemas_ids'] = []
            for result in results:
                logger.info("Adds schema ids")
                queue['message_schemas_ids'].append(result)

            return {
                IdType.QueueID: await self._create_new_queue(queue),
                IdType.SchemasIDs: results
            }

        elif ("message_schemas_ids" not in queue or queue['message_schemas_ids'] is None or len(queue['message_schemas_ids']) == 0) and schemas is None:
            logger.debug("Schema ids weren't given, and no schemas were given")
            queue['message_schemas_ids'] = []
            return {
                IdType.QueueID: await self._create_new_queue(queue),
                IdType.SchemasIDs: []
            }

    async def update_queue(self, queue: IQueue):
        """Updates a queue document

        :param queue: The queue we want to update
        :type queue: IQueue
        :raises LifeIsASimulation: Raises if the queue doesn't exist in the database
        """
        database = self._server.get_database(MONGO_CONFIG["database_name"])
        collection = database.get_collection(MONGO_CONFIG["queues_collection"])

        queue_doc = await collection.find_one({"queue_name": queue["queue_name"]})

        if queue_doc is None or queue_doc == {}:
            raise LifeIsASimulation(
                "Document doesn't exists. I don't know what's real anymore")

        to_update = {}
        for key in queue_doc.keys():
            if key == "_id":
                continue

            if key not in queue:
                to_update[key] = queue_doc[key]
                continue

            if key == "message_schemas_ids":
                to_update[key] = queue[key]

                schemas_to_delete = []
                for schema_id in queue_doc[key]:
                    if schema_id not in queue[key]:
                        schemas_to_delete.append(schema_id)

                if len(schemas_to_delete) == 0:
                    continue

                await self._remove_redundant_schemas(schemas_to_delete, queue["queue_name"])
                continue

            to_update[key] = queue[key]

        if not to_update == {}:
            await collection.replace_one({"_id": queue_doc["_id"]}, to_update)

    async def _remove_redundant_schemas(self, schemas: List[ObjectId], queue_to_ignore: str):
        to_delete = []
        for schema in schemas:
            if not await self._check_if_schema_needed(schema, queue_to_ignore):
                to_delete.append(schema)

        if len(to_delete) == 0:
            return

        database = self._server.get_database(MONGO_CONFIG["database_name"])
        collection = database.get_collection(
            MONGO_CONFIG["schemas_collection"])

        await collection.delete_many({"_id": {"$in": to_delete}})

    async def _check_if_schema_needed(self, schema: ObjectId, queue_to_ignore: str) -> bool:
        database = self._server.get_database(MONGO_CONFIG["database_name"])
        collection = database.get_collection(MONGO_CONFIG["queues_collection"])

        result = await collection.find_one({"message_schemas_ids": schema, "queue_name": {"$ne": queue_to_ignore}})

        return result is not None and not result == {}

    async def update_schema(self, schemas: Dict[Any, Any], queue_name: str):
        """Updates schemas in the database

        :param schemas: The new schemas
        :type schemas: Dict[Any, Any]
        :param queue_name: The queue the schemas belong to
        :type queue_name: str
        :raises LifeIsASimulation: Raises if the queue document doesn't exists
        """
        database = self._server.get_database(MONGO_CONFIG["database_name"])
        collection = database.get_collection(MONGO_CONFIG["queues_collection"])

        queue_doc: IQueue = await collection.find_one({"queue_name": queue_name})

        if queue_doc is None or queue_doc == {}:
            raise LifeIsASimulation(
                "Document doesn't exists. I don't know what's real anymore. Can't modify schema cause of it.")

        collection = database.get_collection(
            MONGO_CONFIG["schemas_collection"])
        schemas_docs = collection.find(
            {"_id": {"$in": queue_doc["message_schemas_ids"]}})

        i = 0
        to_delete = []
        async for schema_doc in schemas_docs:
            if i < len(schemas):
                await collection.replace_one({"_id": schema_doc["_id"]}, schemas[i])
            else:
                to_delete.append(schema_doc)
            i += 1

        await collection.delete_many({"_id": {"$in": to_delete}})

        i -= 1

        if schemas is not None and i < len(schemas):
            queue_doc["message_schemas_ids"] += await self._create_new_schemas(schemas[i + 1:])
            await self.update_queue(queue_doc)

    async def check_if_document_exists(self, document_id: ObjectId, is_schema: bool) -> bool:
        """Checks if the document exists in the Database

        :param document_id: The ID of the document
        :type document_id: ObjectId
        :param is_schema: Whether it's a schema document or queue document
        :type is_schema: bool
        :return: Whether or not the document exists
        :rtype: bool
        """
        logger.debug(
            f"Checking if the document {repr(document_id)} already exists")
        database = self._server.get_database(MONGO_CONFIG["database_name"])
        collection = database.get_collection(
            MONGO_CONFIG["queues_collection"] if not is_schema else MONGO_CONFIG["schemas_collection"])

        return await collection.find_one({"_id": document_id}) not in (None, {})

    async def _create_new_schemas(self, schemas: List[Dict[Any, Any]]) -> List[ObjectId]:
        """Inserts new schema documents

        :param schemas: The schema documents to insert
        :type schemas: List[Dict[Any, Any]]
        :return: The documents' IDs
        :rtype: List[ObjectId]
        """
        logger.debug("Creating new schemas documents")
        database = self._server.get_database(MONGO_CONFIG["database_name"])
        collection = database.get_collection(
            MONGO_CONFIG["schemas_collection"])

        to_insert = []
        already_exists = []
        for schema in schemas:
            document = await collection.find_one(schema)
            if not document == {} and document is not None:
                already_exists.append(document["_id"])
                continue
            to_insert.append(schema)

        if len(to_insert) == 0:
            return already_exists

        return (await collection.insert_many(to_insert)).inserted_ids + already_exists

    async def _create_new_queue(self, queue: IQueue) -> ObjectId:
        """Creates new Queue document and returns its id

        :param queue: Queue document to insert
        :type queue: IQueue
        :return: Document's ID
        :rtype: ObjectId
        """
        logger.debug("Creating a new queue")
        database = self._server.get_database(MONGO_CONFIG["database_name"])
        collection = database.get_collection(MONGO_CONFIG["queues_collection"])

        return (await collection.insert_one(queue)).inserted_id

    def _is_schema_valid(self, schema: Dict[Any, Any]) -> bool:
        """Check if the schema is valid

        :param schema: The schema we want to insert
        :type schema: Dict[Any, Any]
        :return: True if the schema is valid, False otherwise
        :rtype: bool
        """
        for (key, value) in schema.items():
            if key == "_id":
                continue

            if "type" not in value:
                return False

            if value["type"] not in ["string", "integer", "float", "character", "any",
                                     "string[]", "integer[]", "float[]", "character[]", "any[]",
                                     "dictionary"]:
                return False
        return True
