# coding=utf-8
import re
import math
import logging
from datetime import datetime, timedelta


# ------------------------------------------------------------------------
# TIMEDELTA INTERVALS


def get_now_datetime():
    return datetime.now()


def apply_timedelta_on_datetime(my_datetime, my_timedelta):
    return my_datetime + my_timedelta


def apply_timedelta_on_now(my_timedelta):
    return datetime.now() + my_timedelta


def timedelta_to_ns(my_timedelta):
    return int(my_timedelta.total_seconds() * influx_unit_to_ns_factor('s'))


# ------------------------------------------------------------------------
# pandas TIMESTAMP


def pd_timestamp_to_timestamp(pd_timestamp, precision):
    return {
        'ns': pd_timestamp.value,
        'u': int(math.floor(pd_timestamp.value / 1000)),
        'µ': int(math.floor(pd_timestamp.value / 1000)),
        'ms': int(math.floor(pd_timestamp.value / (1000 * 1000))),
        's': int(math.floor(pd_timestamp.value / (1000 * 1000 * 1000))),
        'm': int(math.floor(pd_timestamp.value / (1000 * 1000 * 1000 * 60))),
        'h': int(math.floor(pd_timestamp.value / (1000 * 1000 * 1000 * 60 * 60))),
    }.get(precision, pd_timestamp.value)


# ------------------------------------------------------------------------
# INFLUX TIME + TIMESTAMPS


def split_influx_time(interval):
    # timestamps in influx format can be suffixed by a precision
    number = int(re.findall(r'\d+', interval)[0])
    unit = interval.replace(str(number), '')
    return {'number': number, 'unit': unit}


# ------------------------------------------------------------------------
# INFLUX RETENTION POLICY DURATION


def influx_rp_duration_to_timedelta(rp_duration):
    match = re.match(r'(?P<hours>.+?)h(?P<minutes>.+?)m(?P<seconds>.+?)s', rp_duration)
    if not match:
        return None
    return influx_interval_to_timedelta(match.groupdict()['hours'] + 'h') \
           + influx_interval_to_timedelta(match.groupdict()['minutes'] + 'm') \
           + influx_interval_to_timedelta(match.groupdict()['seconds'] + 's')


def datetime_max_for_influx_rp(rp_duration):
    my_timedelta = -1 * influx_rp_duration_to_timedelta(rp_duration)
    return apply_timedelta_on_now(my_timedelta)


# ------------------------------------------------------------------------
# INFLUX INTERVALS

def influx_interval_to_nanoseconds(influx_interval):
    number = int(re.findall(r'\d+', influx_interval)[0])
    unit = influx_interval.replace(str(number), '')
    return number * influx_unit_to_ns_factor(unit)


def influx_interval_to_timedelta(influx_interval):
    number = int(re.findall(r'\d+', influx_interval)[0])
    unit = influx_interval.replace(str(number), '')
    return influx_interval_to_timedelta_helper(number, unit)


def influx_interval_to_timedelta_helper(number, unit):
    return {
        # NB: can't do 'ns' as timedelta does not support nanoseconds
        'u': timedelta(microseconds=number),
        'µ': timedelta(microseconds=number),
        'ms': timedelta(milliseconds=number),
        's': timedelta(seconds=number),
        'm': timedelta(minutes=number),
        'h': timedelta(hours=number),
        'd': timedelta(days=number),
        'w': timedelta(weeks=number),
    }.get(unit, timedelta(days=0))


# ------------------------------------------------------------------------
# INFLUX TIMESTAMPS


def influx_timestamp_to_datetime(timestamp):
    # timestamps in influx format can be suffixed by a precision
    parts = split_influx_time(timestamp)
    if parts['unit']:
        timestamp_ns = parts['number'] * influx_unit_to_ns_factor(parts['unit'])
    else:
        timestamp_ns = parts['number']
    return datetime.fromtimestamp(timestamp_ns / 1e9)


# ------------------------------------------------------------------------
# UNITS


def influx_unit_to_ns_factor(unit):
    return {
        'ns': 1,
        'u': 1000,
        'µ': 1000,
        'ms': 1000 * 1000,
        's': 1000 * 1000 * 1000,
        'm': 1000 * 1000 * 1000 * 60,
        'h': 1000 * 1000 * 1000 * 60 * 60,
        'd': 1000 * 1000 * 1000 * 60 * 60 * 24,
        'w': 1000 * 1000 * 1000 * 60 * 60 * 24 * 4,
    }.get(unit)


def timestamp_ns_to_influx_unit(number, unit):
    return {
        'ns': number,
        'u': int(math.floor(number / 1000)),
        'µ': int(math.floor(number / 1000)),
        'ms': int(math.floor(number / (1000 * 1000))),
        's': int(math.floor(number / (1000 * 1000 * 1000))),
        'm': int(math.floor(number / (1000 * 1000 * 1000 * 60))),
        'h': int(math.floor(number / (1000 * 1000 * 1000 * 60 * 60))),
    }.get(unit, number)
