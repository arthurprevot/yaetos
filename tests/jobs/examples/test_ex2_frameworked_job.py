import pytest
from jobs.examples.ex2_frameworked_job import Job
from datetime import datetime


def test_format_datetime_conversion_works() -> None:
    data = "20220101000000"
    expected = "2022-01-01 00:00:00"
    result = Job.format_datetime(data)
    assert expected == result


def test_format_datetime_conversion_fails_format_not_valid() -> None:
    data = "this is not a date"
    with pytest.raises(ValueError):
        result = Job.format_datetime(data)


def test_date_diff_seconds_works() -> None:
    start_date = datetime(year=2000, month=1, day=1)
    end_date = datetime(year=2001, month=1, day=1)
    expected = 366 * 24 * 3600
    result = Job.date_diff_sec(start_date, end_date)
    assert expected == result
