import datetime
import uuid
from dataclasses import dataclass, field
from typing import List

import marshmallow.validate


@dataclass
class Batch:
    batch_id: uuid.UUID = field(
        metadata={"marshmallow_field": marshmallow.fields.UUID()}
    )
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
    data: List = field(default_factory=list)
