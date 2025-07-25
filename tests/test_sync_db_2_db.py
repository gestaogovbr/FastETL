from datetime import datetime, date
from typing import Union
import unittest.mock as mock

import pytest

from fastetl.custom_functions.fast_etl import _build_filter_condition


@pytest.mark.parametrize(
    "date_column, key_column, since_datetime, until_datetime, expected_condition",
    [
        # Test case 1: Using date_column and since_datetime
        (
            "last_updated",
            "id",
            date(2022, 1, 1),
            None,
            "last_updated > '2022-01-01'",
        ),
        # Test case 2: Using date_column and since_datetime
        (
            "last_updated",
            "id",
            datetime(2022, 1, 1, 12, 30, 0),
            None,
            "last_updated > '2022-01-01 12:30:00.000'",
        ),
        # Test case 3: Using date_column and since_datetime with until_datetime
        (
            "last_updated",
            "id",
            date(2022, 1, 1),
            date(2022, 1, 31),
            "last_updated > '2022-01-01' AND last_updated <= '2022-01-31'",
        ),
        # Test case 4: Using date_column with date type since_datetime
        ("last_updated", "id", date(2022, 1, 1), None, "last_updated > '2022-01-01'"),
        # Test case 5: Using date_column with date type since_datetime and until_datetime
        (
            "last_updated",
            "id",
            date(2022, 1, 1),
            date(2022, 1, 31),
            "last_updated > '2022-01-01' AND last_updated <= '2022-01-31'",
        ),
       # Test case 6: Using date_column with datetime type since_datetime and until_datetime
        (
            "last_updated",
            "id",
            datetime(2022, 1, 1, 12,  30, 0),
            datetime(2022, 1, 31, 12, 30, 0),
            "last_updated > '2022-01-01 12:30:00.000' AND last_updated <= '2022-01-31 12:30:00.000'",
        ),
        # Test case 7: Using key_column without date_column
        (
            None,
            "id",
            None,
            None,
            "id > '1'",
        ),
        # Test case 8: Omitting all arguments
        (
            None,
            None,
            None,
            None,
            ValueError,
        ),
        # Test case 9: Using since_datetime without date_column
        (
            None,
            "id",
            date(2022, 1, 1),
            None,
            ValueError,
        ),
        # Test case 10: Using empty string for date_column
        (
            "",
            "id",
            None,
            None,
            ValueError,
        ),
        # Test case 11: Using empty string for key_column
        (
            None,
            "",
            None,
            None,
            ValueError,
        ),
    ],
)
def test_build_filter_condition(
    date_column: str,
    key_column: str,
    since_datetime: Union[datetime, date],
    until_datetime: Union[datetime, date],
    expected_condition: Union[str, Exception],
):
    """
    Tests the _build_filter_condition function with different combinations of arguments.

    Args:
        date_column (str): The name of the date column.
        key_column (str): The name of the key column.
        since_datetime (datetime | date): The since datetime.
        until_datetime (datetime | date): The until datetime.
        expected_condition (str | pytest.fixture): The expected condition or
            expected raised exception type.
    """
    # Mock the dest_hook object
    dest_hook = mock.Mock()
    dest_hook.get_first.side_effect = lambda sql: (None,)
    if date_column:
        dest_hook.get_first.side_effect = lambda sql: (datetime(2022, 1, 1),)
    else:
        dest_hook.get_first.side_effect = lambda sql: (1,)

    if isinstance(expected_condition, str):
        # If expected_condition is a string, we can directly compare it with the result
        _, where_condition = _build_filter_condition(
            dest_hook=dest_hook,
            table="table",
            key_column=key_column,
            date_column=date_column,
            since_datetime=since_datetime,
            until_datetime=until_datetime,
        )
        assert where_condition == expected_condition
    else:
        # If expected_condition is a pytest.raises object, we need to use
        # pytest.raises to catch the exception
        with pytest.raises(expected_condition):
            _build_filter_condition(
                dest_hook,
                "table",
                date_column,
                key_column,
                since_datetime,
                until_datetime,
            )
