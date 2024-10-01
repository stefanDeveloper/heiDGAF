from typing import List
from dataclasses import dataclass, field
import marshmallow.validate
import datetime


@dataclass
class Batch:
    begin_timestamp: datetime.datetime = field(
        metadata={
            "marshmallow_field": marshmallow.fields.DateTime("%Y-%m-%dT%H:%M:%S.%fZ")
        }
    )
    end_timestamp: datetime.datetime = field(
        metadata={
            "marshmallow_field": marshmallow.fields.DateTime("%Y-%m-%dT%H:%M:%S.%fZ")
        }
    )
    data: List[dict] = field(default_factory=list)
