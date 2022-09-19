##############################################
#
##############################################
import os
import json
from pyflink.table import EnvironmentSettings, StreamTableEnvironment, StatementSet

env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)),
    'flink-sql-connector-kafka_2.12-1.14.5.jar')
table_env = StreamTableEnvironment.create(environment_settings=env_settings)
table_env.get_config().get_configuration().set_string("pipeline.jars", "file://{}".format(kafka_jar))
statement_set = table_env.create_statement_set()

def app_properties():
    file_path = '/etc/flink/application_properties.json'
    if os.path.isfile(file_path):
        with open(file_path, 'r') as file:
            contents = file.read()
            print('Contents of ' + file_path)
            print(contents)
            properties = json.loads(contents)
            return properties
    else:
        print('A file at "{}" was not found'.format(file_path))

def create_table_input(input_table: str, input_stream: str, broker: str):
    return f"""
    CREATE TABLE {input_table} (
        `customer` VARCHAR,
        `transaction_type` VARCHAR,
        `online_payment_amount` VARCHAR, 
        `in_store_payment_amount` VARCHAR, 
        `lat` VARCHAR, 
        `lon` VARCHAR, 
        `transaction_datetime` VARCHAR
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{input_stream}',
        'properties.bootstrap.servers' = '{broker}',
        'properties.group.id' = 'testGroup',
        'format' = 'json',
        'scan.startup.mode' = 'latest-offset'
    )
    """
def create_table_output_s3(table_name, stream_name):
    return f"""
    CREATE TABLE {table_name} (
        `customer` VARCHAR,
        `transaction_type` VARCHAR
    ) WITH (
        'connector' = 'filesystem',
        'path' = 'file:///home/output/',
        'format' = 'json',
        'sink.partition-commit.policy.kind'='success-file',
        'sink.partition-commit.delay' = '1 min'
    )
    """

def insert_stream_s3(insert_from, insert_into):
    return f"""INSERT INTO {insert_into} SELECT customer, transaction_type FROM {insert_from}"""

def main():

    input_table = "transactions_data_tbl"
    input_stream = "transactions-data"
    broker = "localhost:9092"

    output_table = "transanctions_reduced"

    table_env.execute_sql(create_table_input(input_table, input_stream, broker))
    table_env.execute_sql(create_table_output_s3(output_table, input_stream))

    statement_set.add_insert_sql(insert_stream_s3(input_table, output_table))

    statement_set.execute()


if __name__ == '__main__':
    main()