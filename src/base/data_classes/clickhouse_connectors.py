import datetime
import uuid
from dataclasses import dataclass, field
from typing import Optional

import marshmallow.validate


@dataclass
class ServerLogs:
    message_id: uuid.UUID = field(
        metadata={"marshmallow_field": marshmallow.fields.UUID()}
    )
    timestamp_in: datetime.datetime = field(
        metadata={
            "marshmallow_field": marshmallow.fields.DateTime("%Y-%m-%d %H:%M:%S.%f")
        }
    )
    message_text: str = field(
        metadata={"marshmallow_field": marshmallow.fields.String()}
    )


@dataclass
class ServerLogsTimestamps:
    message_id: uuid.UUID = field(
        metadata={"marshmallow_field": marshmallow.fields.UUID()}
    )
    event: str = field(metadata={"marshmallow_field": marshmallow.fields.String()})
    event_timestamp: datetime.datetime = field(
        metadata={
            "marshmallow_field": marshmallow.fields.DateTime("%Y-%m-%d %H:%M:%S.%f")
        }
    )


@dataclass
class FailedLoglines:
    message_text: str = field(
        metadata={"marshmallow_field": marshmallow.fields.String()}
    )
    timestamp_in: datetime.datetime = field(
        metadata={
            "marshmallow_field": marshmallow.fields.DateTime("%Y-%m-%d %H:%M:%S.%f")
        }
    )
    timestamp_failed: datetime.datetime = field(
        metadata={
            "marshmallow_field": marshmallow.fields.DateTime("%Y-%m-%d %H:%M:%S.%f")
        }
    )
    reason_for_failure: Optional[str] = field(
        metadata={"marshmallow_field": marshmallow.fields.String(allow_none=True)}
    )


@dataclass
class LoglineToBatches:
    logline_id: uuid.UUID = field(
        metadata={"marshmallow_field": marshmallow.fields.UUID()}
    )
    batch_id: uuid.UUID = field(
        metadata={"marshmallow_field": marshmallow.fields.UUID()}
    )


@dataclass
class Loglines:
    logline_id: uuid.UUID = field(
        metadata={"marshmallow_field": marshmallow.fields.UUID()}
    )
    subnet_id: str = field(metadata={"marshmallow_field": marshmallow.fields.String()})
    timestamp: datetime.datetime = field(
        metadata={
            "marshmallow_field": marshmallow.fields.DateTime("%Y-%m-%d %H:%M:%S.%f")
        }
    )
    src_ip: str = field(metadata={"marshmallow_field": marshmallow.fields.String()})
    additional_fields: Optional[str] = field(
        metadata={"marshmallow_field": marshmallow.fields.String(allow_none=True)}
    )


@dataclass
class LoglineTimestamps:
    logline_id: uuid.UUID = field(
        metadata={"marshmallow_field": marshmallow.fields.UUID()}
    )
    stage: str = field(metadata={"marshmallow_field": marshmallow.fields.String()})
    status: str = field(metadata={"marshmallow_field": marshmallow.fields.String()})
    timestamp: datetime.datetime = field(
        metadata={
            "marshmallow_field": marshmallow.fields.DateTime("%Y-%m-%d %H:%M:%S.%f")
        }
    )
    is_active: bool = field(
        metadata={"marshmallow_field": marshmallow.fields.Boolean()}
    )


@dataclass
class BatchTimestamps:
    batch_id: uuid.UUID = field(
        metadata={"marshmallow_field": marshmallow.fields.UUID()}
    )
    instance_name: str = field(
        metadata={"marshmallow_field": marshmallow.fields.String()}
    )
    stage: str = field(metadata={"marshmallow_field": marshmallow.fields.String()})
    status: str = field(metadata={"marshmallow_field": marshmallow.fields.String()})
    timestamp: datetime.datetime = field(
        metadata={
            "marshmallow_field": marshmallow.fields.DateTime("%Y-%m-%d %H:%M:%S.%f")
        }
    )
    is_active: bool = field(
        metadata={"marshmallow_field": marshmallow.fields.Boolean()}
    )
    message_count: int = field(
        metadata={"marshmallow_field": marshmallow.fields.Integer()}
    )


@dataclass
class SuspiciousBatchesToBatch:
    suspicious_batch_id: uuid.UUID = field(
        metadata={"marshmallow_field": marshmallow.fields.UUID()}
    )
    batch_id: uuid.UUID = field(
        metadata={"marshmallow_field": marshmallow.fields.UUID()}
    )


@dataclass
class BatchTree:
    batch_row_id: str = field(
        metadata={"marshmallow_field": marshmallow.fields.String()}
    )
    batch_id: uuid.UUID = field(
        metadata={"marshmallow_field": marshmallow.fields.UUID()}
    )
    parent_batch_row_id: Optional[str] = field(
        metadata={"marshmallow_field": marshmallow.fields.String(allow_none=True)}
    )
    instance_name: str = field(
        metadata={"marshmallow_field": marshmallow.fields.String()}
    )
    stage: str = field(metadata={"marshmallow_field": marshmallow.fields.String()})
    status: str = field(metadata={"marshmallow_field": marshmallow.fields.String()})
    timestamp: datetime.datetime = field(
        metadata={
            "marshmallow_field": marshmallow.fields.DateTime("%Y-%m-%d %H:%M:%S.%f")
        }
    )


@dataclass
class SuspiciousBatchTimestamps:
    suspicious_batch_id: uuid.UUID = field(
        metadata={"marshmallow_field": marshmallow.fields.UUID()}
    )
    src_ip: str = field(metadata={"marshmallow_field": marshmallow.fields.String()})
    instance_name: str = field(
        metadata={"marshmallow_field": marshmallow.fields.String()}
    )
    stage: str = field(metadata={"marshmallow_field": marshmallow.fields.String()})
    status: str = field(metadata={"marshmallow_field": marshmallow.fields.String()})
    timestamp: datetime.datetime = field(
        metadata={
            "marshmallow_field": marshmallow.fields.DateTime("%Y-%m-%d %H:%M:%S.%f")
        }
    )
    is_active: bool = field(
        metadata={"marshmallow_field": marshmallow.fields.Boolean()}
    )
    message_count: int = field(
        metadata={"marshmallow_field": marshmallow.fields.Integer()}
    )


@dataclass
class Alerts:
    src_ip: str = field(metadata={"marshmallow_field": marshmallow.fields.String()})
    suspicious_batch_id: uuid.UUID = field(
        metadata={"marshmallow_field": marshmallow.fields.UUID()}
    )
    alert_timestamp: datetime.datetime = field(
        metadata={
            "marshmallow_field": marshmallow.fields.DateTime("%Y-%m-%d %H:%M:%S.%f")
        }
    )
    overall_score: float = field(
        metadata={"marshmallow_field": marshmallow.fields.Float()}
    )
    domain_names: str = field(
        metadata={"marshmallow_field": marshmallow.fields.String()}
    )
    result: str = field(metadata={"marshmallow_field": marshmallow.fields.String()})


@dataclass
class FillLevels:
    timestamp: datetime.datetime = field(
        metadata={
            "marshmallow_field": marshmallow.fields.DateTime("%Y-%m-%d %H:%M:%S.%f")
        }
    )
    stage: str = field(metadata={"marshmallow_field": marshmallow.fields.String()})
    entry_type: str = field(metadata={"marshmallow_field": marshmallow.fields.String()})
    entry_count: int = field(
        metadata={"marshmallow_field": marshmallow.fields.Integer()}
    )


TABLE_NAME_TO_TYPE = {
    "server_logs": ServerLogs,
    "server_logs_timestamps": ServerLogsTimestamps,
    "failed_loglines": FailedLoglines,
    "logline_to_batches": LoglineToBatches,
    "loglines": Loglines,
    "logline_timestamps": LoglineTimestamps,
    "batch_timestamps": BatchTimestamps,
    "suspicious_batches_to_batch": SuspiciousBatchesToBatch,
    "suspicious_batch_timestamps": SuspiciousBatchTimestamps,
    "alerts": Alerts,
    "fill_levels": FillLevels,
    "batch_tree": BatchTree,
}
