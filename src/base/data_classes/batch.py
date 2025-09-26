import datetime
import uuid
from dataclasses import dataclass, field
from typing import List

import marshmallow.validate


@dataclass
class Batch:
    """
    Class definition of a batch, used to divide the log input into smaller amounts
    """

    batch_tree_row_id: str = field(
        metadata={"marshmallow_field": marshmallow.fields.String()}
    )
    batch_id: uuid.UUID = field(
        metadata={"marshmallow_field": marshmallow.fields.UUID()}
    )
    begin_timestamp: datetime.datetime = field(
        metadata={"marshmallow_field": marshmallow.fields.DateTime()}  # uses ISO format
    )
    end_timestamp: datetime.datetime = field(
        metadata={"marshmallow_field": marshmallow.fields.DateTime()}  # uses ISO format
    )
    data: List = field(default_factory=list)
