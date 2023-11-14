from read_mqtt import process_data, process_message
from unittest.mock import Mock

def test_process_data(data, transformedData):

    assert process_data(data, 'logger/1') == transformedData

def test_process_data_raises_error_on_invalid_data(transformedData, caplog):
    data = {'t': '1609459200', 'id': 'AI01', 'v': '12.5'}
    assert process_data(data, 'logger1') == None
    assert 'Error while processing data: invalid literal for int() with base 10: \'logger1\'' in caplog.text

def test_process_message(data, transformedData, mocker):
    mock_process_data = mocker.patch('read_mqtt.process_data')
    mock_process_data.return_value = transformedData
    mock_message = Mock()
    mock_message.topic = 'logger/1'
    mock_message.payload.decode.return_value = '{"t": "1609459200", "id": "AI01", "v": 12.5}'

    assert process_message(mock_message) == transformedData
    mock_process_data.assert_called_once_with(data, 'logger/1')
    mock_message.payload.decode.assert_called_once_with()

def test_process_message_raises_error_on_invalid_data(transformedData, data, caplog, mocker):
    mock_process_data = mocker.patch('read_mqtt.process_data')
    mock_process_data.return_value = transformedData
    mock_message = Mock()
    mock_message.topic = 'logger/1'
    mock_message.payload.decode.return_value = 'not valid json'

    process_message(mock_message)
    mock_message.payload.decode.assert_called_once_with()
    assert 'Error while decoding JSON: Expecting value: line 1 column 1 (char 0)' in caplog.text