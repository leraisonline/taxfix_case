from unittest.mock import MagicMock, patch

import pytest
from requests.adapters import HTTPAdapter

from src.data_processor import DataProcessor


@pytest.fixture
def init_data_processor():
    return DataProcessor(
        api_url="https://fakerapi.it/api/v2/persons",
        db_path=":memory:",
        total_quantity=100,
        chunk_size=50,
    )


@pytest.fixture
def sample_data():
    return [
        {
            "id": 1,
            "firstname": "John",
            "lastname": "Doe",
            "email": "john.doe@example.com",
            "birthday": "1990-01-01",
            "address": {"street": "123 Main St", "city": "Anytown", "country": "USA"},
        },
        {
            "id": 2,
            "firstname": "Jane",
            "lastname": "Smith",
            "email": "jane.smith@example.com",
            "birthday": "1985-05-15",
            "address": {
                "street": "456 Elm St",
                "city": "Othertown",
                "country": "Canada",
            },
        },
    ]


def test_create_retry_session(init_data_processor):
    session = init_data_processor.create_retry_session()
    assert session is not None
    assert isinstance(session.adapters["http://"], HTTPAdapter)
    assert isinstance(session.adapters["https://"], HTTPAdapter)


@patch("requests.Session.get")
def test_fetch_data_chunk(mock_get, init_data_processor, sample_data):
    mock_response = MagicMock()
    mock_response.json.return_value = {"data": sample_data}
    mock_get.return_value = mock_response

    result = init_data_processor.fetch_data_chunk({"_quantity": 2})
    assert result == sample_data
    mock_get.assert_called_once()


def test_validate_data(init_data_processor, sample_data):
    result = init_data_processor.validate_data(sample_data)
    assert len(result) == 2
    assert all(item in result for item in sample_data)


def test_validate_email(init_data_processor):
    assert init_data_processor.validate_email("test@example.com")
    assert not init_data_processor.validate_email("invalid-email")


def test_validate_date(init_data_processor):
    assert init_data_processor.validate_date("2021-01-01")
    assert not init_data_processor.validate_date("invalid-date")


def test_validate_address(init_data_processor):
    valid_address = {"street": "123 Main St", "city": "Anytown", "country": "USA"}
    invalid_address = {"street": "123 Main St", "country": "USA"}
    assert init_data_processor.validate_address(valid_address)
    assert not init_data_processor.validate_address(invalid_address)


def test_clean_data(init_data_processor, sample_data):
    result = init_data_processor.clean_data(sample_data)
    assert result[0]["email"] == "john.doe@example.com"
    assert result[0]["firstname"] == "John"
    assert result[1]["lastname"] == "Smith"


def test_detect_duplicates(init_data_processor):
    data = [
        {
            "id": 1,
            "email": "test@example.com",
            "address": {"city": "New York", "country": "USA"},
        },
        {
            "id": 2,
            "email": "test@example.com",
            "address": {"city": "New York", "country": "USA"},
        },
        {
            "id": 3,
            "email": "other@example.com",
            "address": {"city": "London", "country": "UK"},
        },
    ]
    unique, duplicates = init_data_processor.detect_duplicates(data)
    assert len(unique) == 2
    assert len(duplicates) == 1


def test_anonymize_data(init_data_processor, sample_data):
    result = init_data_processor.anonymize_data(sample_data)
    assert result[0]["firstname"] == "****"
    assert result[0]["email"] == "****@example.com"
    assert "age_group" in result[0]
    assert result[0]["city"] == "Anytown"
    assert result[0]["country"] == "USA"


def test_calculate_age_group(init_data_processor):
    age_group = init_data_processor.calculate_age_group("1990-01-01")
    assert age_group == "[30-39]"  # Assuming current year is between 2020 and 2029


@patch("sqlite3.connect")
def test_store_data(mock_connect, init_data_processor, sample_data):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_conn

    init_data_processor.store_data(sample_data)

    mock_connect.assert_called_once_with(init_data_processor.db_path)
    mock_cursor.execute.assert_called()  # Check that execute was called on the cursor
    mock_conn.commit.assert_called()
    mock_conn.close.assert_called_once()


def test_profile_data(init_data_processor, sample_data):
    sample_data_with_required_fields = [
        {
            "city": "Anytown",
            "country": "USA",
            "age_group": "[30-39]",
            "email_provider": "example.com",
        },
        {
            "city": "Othertown",
            "country": "Canada",
            "age_group": "[30-39]",
            "email_provider": "example.com",
        },
    ]

    result = init_data_processor.profile_data(sample_data_with_required_fields)
    assert "total_records" in result
    assert result["total_records"] == 2
    assert "unique_values" in result
    assert "most_common_countries" in result
    assert "age_group_distribution" in result
    assert "top_email_providers" in result


@patch("src.data_processor.DataProcessor.fetch_data")
@patch("src.data_processor.DataProcessor.store_data")
def test_process_pipeline(mock_store, mock_fetch, init_data_processor, sample_data):
    mock_fetch.return_value = sample_data
    init_data_processor.process_pipeline()
    mock_fetch.assert_called_once()
    mock_store.assert_called_once()


if __name__ == "__main__":
    pytest.main()
