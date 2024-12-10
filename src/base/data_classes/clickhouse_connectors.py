import datetime
import uuid
from dataclasses import dataclass, field
from typing import Optional

import marshmallow.validate


@dataclass
class ServerLogs:
    message_text: str = field(
        metadata={"marshmallow_field": marshmallow.fields.String()}
    )
    message_id: uuid.UUID = field(
        metadata={"marshmallow_field": marshmallow.fields.UUID()}
    )
    timestamp_in: datetime.datetime = field(
        metadata={
            "marshmallow_field": marshmallow.fields.DateTime("%Y-%m-%d %H:%M:%S.%f")
        }
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
class FailedDNSLoglines:
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
class DNSLoglines:
    logline_id: uuid.UUID = field(
        metadata={"marshmallow_field": marshmallow.fields.UUID()}
    )
    subnet_id: str = field(metadata={"marshmallow_field": marshmallow.fields.String()})
    timestamp: datetime.datetime = field(
        metadata={
            "marshmallow_field": marshmallow.fields.DateTime("%Y-%m-%d %H:%M:%S.%f")
        }
    )
    status_code: str = field(
        metadata={"marshmallow_field": marshmallow.fields.String()}
    )
    client_ip: str = field(metadata={"marshmallow_field": marshmallow.fields.String()})
    record_type: str = field(
        metadata={"marshmallow_field": marshmallow.fields.String()}
    )
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


TABLE_NAME_TO_TYPE = {
    "server_logs": ServerLogs,
    "server_logs_timestamps": ServerLogsTimestamps,
    "failed_dns_loglines": FailedDNSLoglines,
    "logline_to_batches": LoglineToBatches,
    "dns_loglines": DNSLoglines,
    "logline_timestamps": LoglineTimestamps,
    "batch_timestamps": BatchTimestamps,
}
