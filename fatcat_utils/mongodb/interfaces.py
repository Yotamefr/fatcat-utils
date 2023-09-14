from typing import List, Optional, Dict, Any
from typing_extensions import TypedDict
from bson import ObjectId


class IQueue(TypedDict):
    _id: Optional[ObjectId]
    queue_name: str
    message_schemas_ids: List[str]
    tags: List[str]


class IQueueAgg(TypedDict):
    _id: ObjectId
    tags: List[str]
    queue_name: str
    schemas: List[Dict[Any, Any]]