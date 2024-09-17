from typing import Optional, List, Dict
from dataclasses import dataclass, field
import marshmallow_dataclass
import marshmallow.validate
import datetime


@dataclass
class Batch:
    begin_timestamp: datetime.datetime = field()
    end_timestamp: datetime.datetime
    messages: List[dict] = field(default_factory=list)
