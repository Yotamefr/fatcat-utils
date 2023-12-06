from enum import Enum, auto


class AckTime(Enum):
    Start = auto()
    End = auto()
    Manual = auto()


class MessageStatus(Enum):
    Acked = auto()
    Unacked = auto()
    Rejected = auto()
