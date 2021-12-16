"""
Utility functions to mess with date time.
"""

import datetime


def string_to_datetime(date: str) -> datetime:
    """
    Converts a string to a datetime object.
    @param date: Date string in form '%Y-%m-%d_%H:%M:%S'
    @return: datetime corresponding to the input date string.
    """
    return datetime.datetime.strptime(date, '%Y-%m-%d_%H:%M:%S')


def datetime_to_string(dtime):
    timestamp = '{:%Y-%m-%d_%H:%M:%S}'.format(dtime)
    return timestamp


