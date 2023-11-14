from read_mqtt import on_connect, on_message, mqtt_client, subscribe_to_topics
from unittest.mock import Mock

def test_on_connect(mocker, caplog):
    response_codes = {0: 'Connection successful',
                      1: 'Connection refused - incorrect protocol version',
                      2: 'Connection refused - invalid client identifier',
                      3: 'Connection refused - server unavailable',
                      4: 'Connection refused - bad username or password',
                      5: 'Connection refused - not authorized'}
    for code, response in response_codes.items():
        on_connect(None, None, None, code)
        assert f'connected to broker: {response}' in caplog.text

def test_on_message(mocker):
    dbPassword = 'mock_password'
    # Arrange
    mock_client = Mock()
    mock_message = Mock()
    mock_message.payload.decode.return_value = '{"key": "value"}'

    mock_process_message = mocker.patch('read_mqtt.process_message')
    mock_process_message.return_value = 'transformed data'

    mock_insert_data = mocker.patch('read_mqtt.insert_data')
    mocker.patch('read_mqtt.get_db_password', return_value='mock_password')

    # Act
    on_message(mock_client, None, mock_message)

    # Assert
    mock_process_message.assert_called_once_with(mock_message)
    mock_insert_data.assert_called_once_with('transformed data', dbPassword)
    mock_client.publish.assert_called_once_with('pong', 'received message')

def test_mqtt_client(mocker):
    # Arrange
    mock_client = Mock()
    mocker.patch('paho.mqtt.client.Client', return_value=mock_client)


    host = 'test_host'
    port = 1234
    username = 'test_username'
    password = 'test_password'

    # Act
    mqtt_client(host, port, username, password)

    # Assert
    mock_client.username_pw_set.assert_called_once_with(username, password)
    mock_client.connect.assert_called_once_with(host, port=port)

def test_subscribe_to_topics(mocker):
    # Arrange
    mock_client = Mock()
    topics = ['topic1', 'topic2', 'topic3']

    # Act
    subscribe_to_topics(mock_client, topics)

    # Assert
    assert mock_client.subscribe.call_count == len(topics)
    for topic in topics:
        mock_client.subscribe.assert_any_call(topic)