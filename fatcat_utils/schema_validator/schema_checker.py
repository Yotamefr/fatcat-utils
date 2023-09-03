from typing import Any, Dict, Iterable
import re
from fatcat_utils.logger import logger


def _validate_numbers(value: int, schema_value: dict) -> bool:
    """Fot internal use only. Check if a value matches the schema restrictions

    :param value: An integer. We get it from the message.
    :type value: int
    :param schema_value: The schema restrictions for the value.
    :type schema_value: dict
    :return: True if matches, False otherwise
    :rtype: bool
    """
    if "gt" in schema_value:
        if "lt" in schema_value:
            return schema_value["gt"] < value < schema_value["lt"]
        elif "lte" in schema_value:
            return schema_value["gt"] < value <= schema_value["lte"]
        else:
            return schema_value["gt"] < value
    elif "gte" in schema_value:
        if "lt" in schema_value:
            return schema_value["gte"] <= value < schema_value["lt"]
        elif "lte" in schema_value:
            return schema_value["gte"] <= value <= schema_value["lte"]
        else:
            return schema_value["gte"] <= value
    else:
        if "lt" in schema_value:
            return value < schema_value["lt"]
        elif "lte" in schema_value:
            return value <= schema_value["lte"]
        else:
            return True


async def match_to_schema(body: Dict[Any, Any], schema: Dict[Any, Any]) -> Dict[Any, Any]:
    """Check is the message corresponds to the schema, and changes types accordingly.

    :param body: The message body
    :type body: Dict[Any, Any]
    :param schema: The schema
    :type schema: Dict[Any, Any]
    :return: The body of the message after matching it with the schema
    :rtype: Dict[Any, Any]
    """
    logger.debug("Checking if message matches the provided schema")

    if "FATCAT_QUEUE_TAG" in body:
        logger.debug("Removing `FATCAT_QUEUE_TAG` from the message")
        del body["FATCAT_QUEUE_TAG"]
    
    if "_id" in schema:
        logger.debug("Removing `_id` from the schema")
        del schema["_id"]
    
    if not body.keys() == schema.keys():
        logger.info("Message doesn't match because the keys don't match.")
        return
    logger.info("Keys match between the message and the schema")
    
    for (key, value) in schema.items():
        
        if value["type"] == "any":
            logger.info("The schema declares that any object is valid, so the message automatically passed this check")
            continue

        elif value["type"] == "string":
            if body[key] is None:
                logger.info("The value is a None so it matches the schema automatically")
                continue

            body[key] = str(body[key])

            if "match" in value and re.match(re.compile(value["match"]), body[key]) is None:
                logger.info("The key type is a string but it doesn't match the schema")
                return
        
        elif value["type"] == "integer":
            try:
                body[key] = int(body[key])
            except:
                logger.info("The schema declares an integer but the value in the message isn't an integer")
                return
            
            if not _validate_numbers(body[key], value):
                logger.info("The value in the message doesn't match the restrictions provided by the schema")
                return

        elif value["type"] == "float":
            try:
                body[key] = float(body[key])
            except:
                logger.info("The schema declares a float but the value in the message isn't a float")
                return
            
            if not _validate_numbers(body[key], value):
                logger.info("The value in the message doesn't match the restrictions provided by the schema")
                return

        elif value["type"] == "boolean":
            try:
                body[key] = bool(body[key])
            except Exception as e:
                logger.info("The schema declares a boolean but the value in the message isn't a boolean", exc_info=e)
                return

        elif value["type"] == "character":
            if body[key] is None:
                continue

            body[key] = str(body[key])
            if not len(body[key]) == 1:
                logger.info("The value in the message isn't a character")
                return

        # ARRAYS!
        elif value["type"] == "any[]":
            logger.info("The schema declares a list of any object, so the message automatically passed this check")
            continue

        elif value["type"] == "string[]":
            if not isinstance(body[key], Iterable) or isinstance(body[key], str):
                logger.info("The value in the message isn't an iterable")
                return

            for i in range(len(body[key])):
                body[key][i] = str(body[key][i])

                if "match" in value and re.match(re.compile(value["match"]), body[key][i]) is None:
                    logger.info("The key type is a string but it doesn't match the schema")
                    return

        elif value["type"] == "integer[]":
            if not isinstance(body[key], Iterable) or isinstance(body[key], str):
                logger.info("The value in the message isn't an iterable")
                return
            
            for i in range(len(body[key])):
                try:
                    body[key][i] = int(body[key][i])
                except:
                    logger.info("The schema declares an integer but the value in the message isn't an integer")
                    return
            
                if not _validate_numbers(body[key][i], value):
                    logger.info("The value in the message doesn't match the restrictions provided by the schema")
                    return

        elif value["type"] == "float[]":
            if not isinstance(body[key], Iterable) or isinstance(body[key], str):
                logger.info("The value in the message isn't an iterable")
                return
            
            for i in range(len(body[key])):
                try:
                    body[key][i] = float(body[key][i])
                except:
                    logger.info("The schema declares a float but the value in the message isn't a float")
                    return
            
                if not _validate_numbers(body[key][i], value):
                    logger.info("The value in the message doesn't match the restrictions provided by the schema")
                    return

        elif value["type"] == "boolean[]":  # Why would anyone even need this
            if not isinstance(body[key], Iterable) or isinstance(body[key], str):
                logger.info("The value in the message isn't an iterable")
                return
            
            for i in range(len(body[key])):
                try:
                    body[key][i] = bool(body[key][i])
                except:
                    logger.info("The schema declares a boolean but the value in the message isn't a boolean")
                    return

        elif value["type"] == "character[]":  # This is practically a string though
            if not isinstance(body[key], Iterable) or isinstance(body[key], str):
                logger.info("The value in the message isn't an iterable")
                return
            
            for i in range(len(body[key])):
                if body[key][i] is None:
                    continue

                body[key][i] = str(body[key][i])
                if not len(body[key]) == 1:
                    logger.info("The value in the message isn't a character")
                    return
        
        # DICTIONARIES!
        elif value["type"] == "dictionary":
            if not isinstance(body[key], dict):
                logger.info("The value in the message isn't a dictionary")
                return
            
            logger.info("Going deeper. The value was a dictionary so now we go an check the inner schema")
            return await match_to_schema(body[key], value["inner_schema"])
        
        else:
            logger.error("I don't know what you set in the schema so I say it doesn't match. It's your fault, really.")
            return
    
    return body