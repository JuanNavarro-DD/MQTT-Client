import pytest

@pytest.fixture
def transformedData() -> dict:
    return {'timestamp': ['2021-01-01 08:00:00'], 'site_id': [1], 'log_type': ['AI'], 'tag': ['AI01'], 'value': [12.5]}

@pytest.fixture
def data() -> dict:
    return {'t': '1609459200', 'id': 'AI01', 'v': 12.5}