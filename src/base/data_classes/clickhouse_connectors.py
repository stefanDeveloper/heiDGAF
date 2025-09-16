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
        metadata={"marshmallow_field": marshmallow.fields.DateTime()}
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
        metadata={"marshmallow_field": marshmallow.fields.DateTime()}
    )


@dataclass
class FailedDNSLoglines:
    message_text: str = field(
        metadata={"marshmallow_field": marshmallow.fields.String()}
    )
    timestamp_in: datetime.datetime = field(
        metadata={"marshmallow_field": marshmallow.fields.DateTime()}
    )
    timestamp_failed: datetime.datetime = field(
        metadata={"marshmallow_field": marshmallow.fields.DateTime()}
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
        metadata={"marshmallow_field": marshmallow.fields.DateTime()}
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
        metadata={"marshmallow_field": marshmallow.fields.DateTime()}
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
        metadata={"marshmallow_field": marshmallow.fields.DateTime()}
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
class SuspiciousBatchTimestamps:
    suspicious_batch_id: uuid.UUID = field(
        metadata={"marshmallow_field": marshmallow.fields.UUID()}
    )
    client_ip: str = field(metadata={"marshmallow_field": marshmallow.fields.String()})
    stage: str = field(metadata={"marshmallow_field": marshmallow.fields.String()})
    status: str = field(metadata={"marshmallow_field": marshmallow.fields.String()})
    timestamp: datetime.datetime = field(
        metadata={"marshmallow_field": marshmallow.fields.DateTime()}
    )
    is_active: bool = field(
        metadata={"marshmallow_field": marshmallow.fields.Boolean()}
    )
    message_count: int = field(
        metadata={"marshmallow_field": marshmallow.fields.Integer()}
    )


@dataclass
class Alerts:
    client_ip: str = field(metadata={"marshmallow_field": marshmallow.fields.String()})
    suspicious_batch_id: uuid.UUID = field(
        metadata={"marshmallow_field": marshmallow.fields.UUID()}
    )
    alert_timestamp: datetime.datetime = field(
        metadata={"marshmallow_field": marshmallow.fields.DateTime()}
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
        metadata={"marshmallow_field": marshmallow.fields.DateTime()}
    )
    stage: str = field(metadata={"marshmallow_field": marshmallow.fields.String()})
    entry_type: str = field(metadata={"marshmallow_field": marshmallow.fields.String()})
    entry_count: int = field(
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
    "suspicious_batches_to_batch": SuspiciousBatchesToBatch,
    "suspicious_batch_timestamps": SuspiciousBatchTimestamps,
    "alerts": Alerts,
    "fill_levels": FillLevels,
}
