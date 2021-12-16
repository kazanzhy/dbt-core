from abc import ABCMeta
from dataclasses import dataclass, field
from dbt.events.serialization import dbtClassEventMixin
from datetime import datetime
import os
import threading
from typing import Any, Optional, Dict

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# These base types define the _required structure_ for the concrete event #
# types defined in types.py                                               #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


class Cache():
    # Events with this class will only be logged when the `--log-cache-events` flag is passed
    pass


@dataclass
class ShowException():
    # N.B.:
    # As long as we stick with the current convention of setting the member vars in the
    # `message` method of subclasses, this is a safe operation.
    # If that ever changes we'll want to reassess.
    def __post_init__(self):
        self.exc_info: Any = True
        self.stack_info: Any = None
        self.extra: Any = None


# TODO add exhaustiveness checking for subclasses
# top-level superclass for all events
@dataclass
class Event(dbtClassEventMixin, metaclass=ABCMeta):
    # fields that should be on all events with their default implementations
    log_version: int = 1
    ts: Optional[datetime] = None  # use getter for non-optional
    ts_rfc3339: Optional[str] = None  # use getter for non-optional
    pid: Optional[int] = None  # use getter for non-optional

    # four digit string code that uniquely identifies this type of event
    # uniqueness and valid characters are enforced by tests
    @property
    @staticmethod
    def code() -> str:
        raise Exception("code() not implemented for event")

    # do not define this yourself. inherit it from one of the above level types.
    def level_tag(self) -> str:
        raise Exception("level_tag not implemented for Event")

    # Solely the human readable message. Timestamps and formatting will be added by the logger.
    # Must override yourself
    def message(self) -> str:
        raise Exception("msg not implemented for Event")

    # exactly one time stamp per concrete event
    def get_ts(self) -> datetime:
        if not self.ts:
            self.ts = datetime.utcnow()
            self.ts_rfc3339 = self.ts.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        return self.ts

    # preformatted time stamp
    def get_ts_rfc3339(self) -> str:
        if not self.ts_rfc3339:
            # get_ts() creates the formatted string too so all time logic is centralized
            self.get_ts()
        return self.ts_rfc3339  # type: ignore

    # exactly one pid per concrete event
    def get_pid(self) -> int:
        if not self.pid:
            self.pid = os.getpid()
        return self.pid

    # in theory threads can change so we don't cache them.
    def get_thread_name(self) -> str:
        return threading.current_thread().getName()

    @classmethod
    def get_invocation_id(cls) -> str:
        from dbt.events.functions import get_invocation_id
        return get_invocation_id()


# in preparation for #3977
class TestLevel(Event):
    def level_tag(self) -> str:
        return "test"


class DebugLevel(Event):
    def level_tag(self) -> str:
        return "debug"


class InfoLevel(Event):
    def level_tag(self) -> str:
        return "info"


class WarnLevel(Event):
    def level_tag(self) -> str:
        return "warn"


class ErrorLevel(Event):
    def level_tag(self) -> str:
        return "error"


# prevents an event from going to the file
class NoFile():
    pass


# prevents an event from going to stdout
class NoStdOut():
    pass


@dataclass
class NodeInfo():
    node_info: Dict[str, Any] = field(default_factory=dict)
