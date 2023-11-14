from read_mqtt import insert_data, read_logger_config
from unittest.mock import Mock
import pytest
import pandas as pd
import logging

_logger = logging.getLogger(__name__)

def test_insert_data(transformedData, mocker):
    mock_create_engine = mocker.patch('read_mqtt.create_engine')
    mock_to_sql = mocker.patch.object(pd.DataFrame, 'to_sql')

    mock_engine = Mock()
    mock_create_engine.return_value = mock_engine
    _logger.debug(f'calling insert_data with transformedData: {transformedData} and password: password')
    insert_data(transformedData, 'password')
    _logger.debug(f'mock calls: {mock_to_sql.mock_calls}')
    _logger.debug(f'mock calls: {mock_create_engine.mock_calls}')

    mock_create_engine.assert_called_once_with('postgresql://postgres:password@localhost:5432/level_crossing')
    mock_to_sql.assert_called_once_with('transformed_table', mock_engine, if_exists='append', index=False)

def test_insert_data_raises_error_on_connection_failure(transformedData, mocker, caplog):
    mock_create_engine = mocker.patch('read_mqtt.create_engine')
    mock_create_engine.side_effect = Exception('Connection error')

    insert_data(transformedData, 'password')

    assert 'Error while connecting to PostgreSQL: Connection error' in caplog.text

def test_read_logger_config(mocker):
    mock_create_engine = mocker.patch('read_mqtt.create_engine')
    mock_read_sql_query = mocker.patch.object(pd, 'read_sql_query')

    mock_engine = Mock()
    mock_create_engine.return_value = mock_engine
    _logger.debug(f'calling read_logger_config with password: password')
    _logger.debug(f'mock calls: {mock_read_sql_query.mock_calls}')
    _logger.debug(f'mock calls: {mock_create_engine.mock_calls}')
    read_logger_config('password')
    _logger.debug(f'mock calls: {mock_read_sql_query.mock_calls}')
    _logger.debug(f'mock calls: {mock_create_engine.mock_calls}')

    mock_create_engine.assert_called_once_with('postgresql://postgres:password@localhost:5432/level_crossing')
    mock_read_sql_query.assert_called_once_with('SELECT * FROM logger_info', mock_engine)

def test_read_logger_config_raises_error_on_connection_failure(mocker, caplog):
    mock_create_engine = mocker.patch('read_mqtt.create_engine')
    mock_create_engine.side_effect = Exception('Connection error')

    read_logger_config('password')

    assert 'Error while connecting to PostgreSQL select query: Connection error' in caplog.text
