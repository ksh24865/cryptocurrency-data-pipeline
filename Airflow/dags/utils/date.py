import time
from datetime import date, datetime, timedelta
from datetime import timezone as tz
from functools import reduce
from typing import Final, Iterable, Tuple, Union

from dateutil.parser import parse
from pytz import timezone

KST: Final[tz] = timezone("Asia/Seoul")
DAY: Final[timedelta] = timedelta(days=1)


def get_date_for_today() -> date:
    return datetime.now().astimezone(KST).date()


def get_date_months_before(current_date: date, months: int) -> date:
    return reduce(lambda acc, _: a_month_before(acc), range(months), current_date)


def get_date_days_before(current_date: date, days: int) -> date:
    return current_date - timedelta(days=days)


def get_datetime_days_before(current_date: date, days: int) -> datetime:
    return date_to_datetime(
        get_date_days_before(
            current_date=current_date,
            days=days,
        )
    )


def get_datetime_hours_before(current_date: date, hours: int) -> datetime:
    return date_to_datetime(current_date) - timedelta(hours=hours)


def a_month_before(current_date: date) -> date:
    if current_date.month == 1:
        return date(year=current_date.year - 1, month=12, day=current_date.day)
    if current_date.month == 3 and current_date.day > 28:
        if current_date.year % 4 == 0:
            return date(year=current_date.year, month=current_date.month - 1, day=29)
        else:
            return date(year=current_date.year, month=current_date.month - 1, day=28)

    return date(
        year=current_date.year, month=current_date.month - 1, day=current_date.day
    )


def get_date_years_before(current_date: date, years: int) -> date:
    return reduce(lambda acc, _: a_month_before(acc), range(years * 12), current_date)


def date_range(
    start_date: date,
    end_date: date,
    time_interval: int = 1,
) -> Iterable[Tuple[date, date]]:
    print(f"start_date:{start_date}, end_date:{end_date}")
    time_interval = timedelta(days=time_interval)
    tmp_date = start_date + time_interval - DAY
    while tmp_date < end_date:
        yield start_date, tmp_date
        start_date += time_interval
        tmp_date += time_interval
    yield start_date, end_date


def datetime_range(
    start_datetime: datetime,
    end_datetime: datetime,
    time_interval: int = 1,
) -> Iterable[Tuple[datetime, datetime]]:
    print(f"start_datetime:{start_datetime}, end_datetime:{end_datetime}")
    time_interval = timedelta(seconds=time_interval)
    tmp_date = start_datetime + time_interval - timedelta(seconds=1)
    while tmp_date < end_datetime:
        yield start_datetime, tmp_date
        start_datetime += time_interval
        tmp_date += time_interval
    yield start_datetime, end_datetime


def str_to_datetime(date_str: str) -> datetime:
    return parse(date_str)


def str_to_date(date_str: str) -> date:
    return str_to_datetime(date_str).date()


def date_to_datetime(d: date) -> datetime:
    return datetime(
        year=d.year,
        month=d.month,
        day=d.day,
    )


def utc_to_kst(
    utc_date_str: str,
) -> str:
    utc_date = str_to_datetime(utc_date_str)
    kst_date = utc_date.astimezone(KST).date()
    return str(kst_date)


def datetime_to_timestamp(
    dt: Union[date, datetime],
) -> int:
    timestamp = time.mktime(dt.timetuple())
    return int(timestamp)


def str_to_timestamp(
    datetime_str: str,
) -> int:
    return datetime_to_timestamp(str_to_datetime(datetime_str))
