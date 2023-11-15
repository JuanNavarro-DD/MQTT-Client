import paho.mqtt.client as mqtt
import json
import pendulum
from sqlalchemy import create_engine
import pandas as pd
import logging
import cryptocode

def get_db_password():
    with open('dbPassword.txt', 'r') as f:
            return f.read()

_logger = logging.getLogger()
logging.basicConfig(level=logging.DEBUG)

def insert_data(data:dict, dbPassword:str):
    dataDataFrame = pd.DataFrame(data)
    _logger.info(f'dataDataFrame: {dataDataFrame}')
    try:
        engine = create_engine(f'postgresql://postgres:{dbPassword}@localhost:5432/level_crossing')
        dataDataFrame.to_sql('transformed_table', engine, if_exists='append', index=False)
        
    except Exception as error:
        _logger.error(f'Error while connecting to PostgreSQL: {error}')

def read_logger_config(dbPassword:str) -> pd.DataFrame:
    try:
        engine = create_engine(f'postgresql://postgres:{dbPassword}@localhost:5432/level_crossing')
        return pd.read_sql_query('SELECT * FROM logger_info', engine)
    except Exception as error:
        _logger.error(f'Error while connecting to PostgreSQL select query: {error}')



def process_data(data:dict, topic:str) -> dict:
    try:
        transformedData = {}
        transformedData['timestamp'] = [pendulum.from_timestamp(int(data['t']), 'Australia/Perth').to_datetime_string()]
        transformedData['site_id'] = [int(topic.split('/')[-1])]
        transformedData['log_type'] = [data['id'][:2]]
        transformedData['tag'] = [data['id']]
        transformedData['value'] = [data['v']]
        return transformedData
    except Exception as error:
        _logger.error(f'Error while processing data: {error}')
    
def process_message(message):
    try:
        data = json.loads(message.payload.decode())
        topic = message.topic
        return process_data(data, topic)
    except json.JSONDecodeError as error:
        _logger.error(f'Error while decoding JSON: {error}')
    

def on_connect(client, userdata, flags, response_code):
    connackString = {0: 'Connection successful',
                      1: 'Connection refused - incorrect protocol version',
                      2: 'Connection refused - invalid client identifier',
                      3: 'Connection refused - server unavailable',
                      4: 'Connection refused - bad username or password',
                      5: 'Connection refused - not authorized'}
    _logger.critical(f'connected to broker: {connackString[response_code]}')

def on_message(client, userdata, message):
    _logger.info(f'received message: {message.payload.decode()}')
    transformedData = process_message(message)
    dbPassword = get_db_password()
    _logger.info(f'passw: {dbPassword}')
    insert_data(transformedData, dbPassword)
    client.publish('pong', 'received message')

def mqtt_client(host:str, port:int, username:str, password:str):
    mqttClient = mqtt.Client(protocol=mqtt.MQTTv311)
    mqttClient.username_pw_set(username, password)
    mqttClient.on_connect = on_connect
    mqttClient.on_message = on_message
    mqttClient.connect(host, port=port)
    return mqttClient

def subscribe_to_topics(mqttClient, topics:list):
    for topic in topics:
        mqttClient.subscribe(topic)


if __name__ == '__main__':
    dbPassword = get_db_password()
    mqttUserName = 'mqtt'
    loggerInfo:pd.DataFrame = read_logger_config(dbPassword)
    _logger.info(f'loggerInfo: {loggerInfo.to_dict("records")}')
    brokerPassword = cryptocode.decrypt(loggerInfo['logger_password'].values[0], 'level_crossing')
    mqttClient = mqtt_client('192.168.68.114', 1883, mqttUserName, brokerPassword)

    subscribe_to_topics(mqttClient, loggerInfo['logger_topic'].values)

    mqttClient.loop_forever()