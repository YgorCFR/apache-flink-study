import os 

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import JsonRowDeserializationSchema, JsonRowSerializationSchema, Encoder
from pyflink.common.typeinfo import Types
from pyflink.common import Row
from pyflink.table import EnvironmentSettings
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer, FileSink, OutputFileConfig, RollingPolicy

def main():
    # 1. Creating streaming environment
    env = StreamExecutionEnvironment.get_execution_environment()
    #### Defining settings 
    settings = EnvironmentSettings.new_instance()\
                                  .in_streaming_mode()\
                                  .build()
    #### Add kafka connector dependency 
    kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)),
    'flink-sql-connector-kafka_2.12-1.14.5.jar')
    env.add_jars(f"file://{kafka_jar}")

    # 2. Create source DataStream 
    deserialization_schema = JsonRowDeserializationSchema.builder()\
        .type_info(type_info=Types.ROW([
            Types.STRING(), 
            Types.STRING(), 
            Types.STRING(), 
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING()
          ]
        )).build()
    #### Defining Kafka Source
    kafka_source = FlinkKafkaConsumer(
        topics='transactions-data',
        deserialization_schema=deserialization_schema,
        properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group', 'auto.offset.reset' : 'earliest'}
    )
    #### Adding source
    ds = env.add_source(kafka_source)
    ds.print()
    serialization_schema = JsonRowSerializationSchema.builder().with_type_info(type_info=Types.ROW([
            Types.STRING(), 
            Types.STRING(), 
            Types.STRING(), 
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING()
          ]
    )).build()

    ds.sink_to(
            sink=FileSink.for_row_format(
                base_path="/home/ygor/",
                encoder=Encoder.simple_string_encoder())
            .with_output_file_config(
                OutputFileConfig.builder()
                .with_part_prefix("prefix")
                .with_part_suffix(".json")
                .build())
            .with_rolling_policy(RollingPolicy.default_rolling_policy())
            .build()
    )

    ds.print()
    env.execute()    
    
    ##############################################################
    


if __name__ == '__main__':
    main()