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
statement_set_2 = table_env.create_statement_set()

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
        `online_payment_amount` DOUBLE, 
        `in_store_payment_amount` DOUBLE, 
        `lat` DOUBLE, 
        `lon` DOUBLE, 
        `transaction_datetime` TIMESTAMP_LTZ
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{input_stream}',
        'properties.bootstrap.servers' = '{broker}',
        'json.timestamp-format.standard' = 'ISO-8601',
        'properties.group.id' = 'testGroup',
        'format' = 'json',
        'scan.startup.mode' = 'latest-offset'
    )
    """
def create_table_output_s3(table_name, stream_name):
    return f"""
    CREATE TABLE {table_name} (
        `customer` VARCHAR,
        `transaction_type` VARCHAR,
        `transaction_datetime` TIMESTAMP_LTZ,
        `year_rec` BIGINT, 
        `month_rec` BIGINT,
        `day_rec` BIGINT 
    ) PARTITIONED BY (
        year_rec, month_rec, day_rec 
    ) WITH (
        'connector' = 'filesystem',
        'path' = 'file:///home/ygor/output/',
        'format' = 'json',
        'sink.partition-commit.policy.kind'='success-file',
        'sink.partition-commit.delay' = '1 min'
    )
    """

def create_table_2_phase(table_name, stream_name):
    return f"""
    CREATE TABLE {table_name} (
       `customer` VARCHAR,
       `qtde_type` BIGINT NOT NULL 
    ) WITH (
        'connector' = 'filesystem',
        'path' = 'file:///home/ygor/output2/',
        'format' = 'json',
        'sink.partition-commit.policy.kind'='success-file',
        'sink.partition-commit.delay' = '1 min'
    )
    """

def insert_stream_s3_2_phase(insert_from, insert_into):
    return f"""
    INSERT INTO {insert_into} 
    SELECT customer, 1 as qtde_type
    FROM {insert_from}  
    """

def insert_stream_s3(insert_from, insert_into):
    return f"""INSERT INTO {insert_into} 
               SELECT customer, 
                      transaction_type,
                      transaction_datetime, 
                      YEAR(transaction_datetime) as year_rec,
                      MONTH(transaction_datetime) as month_rec,
                      DAYOFMONTH(transaction_datetime) as day_rec  
               FROM {insert_from}"""

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