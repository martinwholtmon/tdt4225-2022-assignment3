"""The dataclasses that are insert into the mongodb"""
from dataclasses import dataclass
from datetime import datetime


@dataclass
class TrackPoint:
    """A trackpoint in the database for an activity"""

    # _id: int
    user_id: str  # Referencing the user
    activity_id: str  # Referencing parent activity
    lat: float
    lon: float
    altitude: int
    date_days: int
    date_time: datetime


@dataclass
class Activity:
    """An activity in the database for a user"""

    # _id: int
    user_id: str  # Referencing parent
    transportation_mode: str
    start_date_time: datetime
    end_date_time: datetime
    # trackpoints: list  # list with _id of trackpoints


@dataclass
class User:
    """A user in the database"""

    _id: str
    has_label: bool
    activities: dict  # List of activities with transportation_mode
