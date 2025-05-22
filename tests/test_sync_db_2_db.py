from datetime import datetime, date
import unittest.mock as mock

import pytest

from fastetl.custom_functions.fast_etl import _build_filter_condition


@pytest.mark.parametrize(
    "date_column, key_column, since_datetime, until_datetime, expected_condition",
    [
        # Test case 1: Using date_column and since_datetime
        (
            "last_updated",
            None,
            datetime(2022, 1, 1),
            None,
            "last_updated > '2022-01-01T00:00:00'",
        ),
        # Test case 2: Using date_column and since_datetime with until_datetime
        (
            "last_updated",
            None,
            datetime(2022, 1, 1),
            datetime(2022, 1, 31),
            "last_updated > '2022-01-01T00:00:00' AND last_updated <= '2022-01-31T00:00:00'",
        ),
        # Test case 3: Using date_column with date type since_datetime
        ("last_updated", None, date(2022, 1, 1), None, "last_updated > '2022-01-01'"),
        # Test case 4: Using date_column with date type since_datetime and until_datetime
        (
            "last_updated",
            None,
            date(2022, 1, 1),
            date(2022, 1, 31),
            "last_updated > '2022-01-01' AND last_updated <= '2022-01-31'",
        ),
        # Test case 5: Using key_column
        (
            None,
            "key_column",
            None,
            None,
            "key_column > '1'",
        ),  # Assuming max_loaded_value is an empty string
        # Test case 6: Omitting all optional arguments
        (None, None, None, None, "1 = 1"),
        # Test case 7: Using neither key_column nor date_column
        (
            None,
            None,
            None,
            None,
            "1 = 1",
        ),  # This should return a filter condition with all values
        # Test case 8: Using since_datetime without date_column
        (
            None,
            "key_column",
            date(2022, 1, 1),
            None,
            ValueError,
        ),  # This should raise a ValueError
        # Test case 9: Using both date_column and key_column
        (
            "date_column",
            "key_column",
            None,
            None,
            ValueError,
        ),  # This should raise a ValueError
        # Test case 10: Using empty string for date_column
        (
            "",
            None,
            None,
            None,
            ValueError,
        ),  # This should raise a ValueError
        # Test case 11: Using empty string for key_column
        (
            None,
            "",
            None,
            None,
            ValueError,
        ),  # This should raise a ValueError
        # Test case 12: Using key_column and since_datetime
        (
            None,
            "key_column",
            datetime(2022, 1, 1),
            None,
            ValueError,
        ),  # This should raise a ValueError
    ],
)
def test_build_filter_condition(
    date_column: str,
    key_column: str,
    since_datetime: datetime | date,
    until_datetime: datetime | date,
    expected_condition: str | Exception,
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
    if not date_column and key_column:
        dest_hook.get_first.side_effect = lambda sql: (1,)
    elif date_column and not key_column:
        dest_hook.get_first.side_effect = lambda sql: (datetime(2022, 1, 1),)


    if isinstance(expected_condition, str):
        # If expected_condition is a string, we can directly compare it with the result
        _, where_condition = _build_filter_condition(
            dest_hook, "table", date_column, key_column, since_datetime, until_datetime
        )
        assert where_condition == expected_condition
    else:
        # If expected_condition is a pytest.raises object, we need to use
        # pytest.raises to catch the exception
        with pytest.raises(expected_condition):
            _build_filter_condition(
                dest_hook, "table", date_column, key_column, since_datetime, until_datetime
            )
