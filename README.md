# apache-flink-study
Repositório dedicado aos estudos de apache-flink

## 001_pyflink_discovering
- Descobrimento do uso do flink. 
- Requisitos:
  - Python 3.7, 3.8, 3.9 , recomendáveis. Crie um ambiente virtual.
  - Java 11.
  - Apache Flink 1.14.
  - Kafka, versão mais recente.
  - Ambiente UNIX. e.g.: Linux Ubuntu, Microsoft WSL.  
- Cenário utilizado:
    - A proposta dessa fase foi desenvolver uma pequena aplicação para conhecer os conceitos da tecnologia. O desenvolvimento desse cenário consistiu em:
      - Alimentar um tópico de kafka fazendo com que o mesmo fosse consumido em tempo real por uma aplicação de Python Flink. 
      - Produzir as informações a partir de um script em Python para que essas fossem enviadas a um tópico Kafka. 
      - Salvar em tempo real e sob demanda as informações que chegam à aplicação de streaming e realizar queries sql. 
      - As informações são salvas no diretório local a partir da utilização do conector de filesystem. 
- Execução:
   1. Nesse cenário não foi utilizado o docker. 
   2. O docker será parte do diretório 002 que ainda será desenvolvido.
   3. Instale o kafka em sua máquina. Link de referência: https://www.digitalocean.com/community/tutorials/how-to-install-apache-kafka-on-ubuntu-20-04 . 
   4. Instale o apache flink, de preferência uma versão instável, a utilizada nessa fase foi o flink 1.14.5. O link de instalação foi: https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/try-flink/local_installation/ . Além disso, o link de instalação recomenda que haja uma versão do Java instalada. Para essa fase foi instalada a versão 11 do Java a partir do comando que pode ser performado em um ambiente UNIX como Linux Ubuntu ou Microsoft WSL: 
   ```bash 
   sudo apt update -y && sudo apt install default-jdk -y
   ```
   5. Após ter o apache flink instalado, será necessário reproduzir o produtor de informações a partir do script de geração de informações. Esse script localiza-se na pasta `001_pyflink_discovering/data-generators/dummy_data_generator.py`.
   ```bash
   python 001_pyflink_discovering/data-generators/dummy_data_generator.py
   ```
   6. As mensagens serão possíveis de se visualizar pelo Kafka a partir do comando, dado um tópico com o nome 'transactions-data':
   ```
   ~/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic transactions-data --from-beginning
   ```
   7. Após ter concluídos os testes com o Kafka, cancele o último comando com o atalho Ctrl + C.
   8. É momento de inicializar o cluster do flink. Inicialize-o com o comando: 
   ```bash
   ./bin/start-cluster.sh 
   ```
   9. Agora, realizar o job em streaming com o comando:
   ```
   ./bin/flink run -py  001_pyflink_discovering/docker-pyflink/examples/main.py 
   ```
   10. Será possível ver os resultados em `~/output/`.
   > Você pode configurar o output na linha 53 do script main.py. 

## 002_pyflink_partitioning
 - Particionamento de tabelas, como salvar as tabelas no Apache Flink particionando-as.
 - Foi alterado o cenário de ingestão onde as colunas passaram a receber um tipo de dado em específico:
 ```python
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
        'path' = 'file:///home/<<USER>>/output/',
        'format' = 'json',
        'sink.partition-commit.policy.kind'='success-file',
        'sink.partition-commit.delay' = '1 min'
    )
    """
 ```
 - Na escrita as partições foram criadas para atender o requisito:
 ```python
 def insert_stream_s3(insert_from, insert_into):
    return f"""INSERT INTO {insert_into} 
               SELECT customer, 
                      transaction_type,
                      transaction_datetime, 
                      YEAR(transaction_datetime) as year_rec,
                      MONTH(transaction_datetime) as month_rec,
                      DAYOFMONTH(transaction_datetime) as day_rec  
               FROM {insert_from}"""
 ```
 - O cenário final gerou um diagrama desse formato:
 ![Partitioning diagram](./images/Screenshot%20from%202022-09-19%2022-54-13.png)

 ## 003_pyflink_aggregation
  - Inserido o tipo de agregação à tabela de transações.
  - Foi utilizada a categoria de agregação TUMBLE.
  - A função de TUMBLE, assinala as linhas À janelas de agregação de tempo não fixadas. Essas janelas não se sobrepõem. 
  - Em uma agregação utilizando GROUP BY na API de Tabelas do Flink torna-se possível realizar uma agregação por janela de tempo vindo a necessidade do uso da função TUMBLE.
  - Ao se realizar essa escolha, é necessário definir qual será coluna que conduzirá essas agregações. A coluna que está exposta à função de TUMBLE deve ser do tipo TIMESTAMP(3) ou TIMESTAMP_LTZ(3).
  - Além disso essa coluna deve estar sob a configuração de WATERMARK na sua origem, ou seja, no CREATE TABLE de comunicação com o source origem. 
  - A função de WATERMARK permite o uso de uma coluna de referência no dataset para que esta seja utilizada no sistema de streaming como definidora da linha do tempo.
  - A alteração necessária para inserir o WATERMARK deu-se na query a seguir:
  ```python
  def create_table_input(input_table: str, input_stream: str, broker: str):
    return f"""
    CREATE TABLE {input_table} (
        `customer` VARCHAR,
        `transaction_type` VARCHAR,
        `online_payment_amount` DOUBLE, 
        `in_store_payment_amount` DOUBLE, 
        `lat` DOUBLE, 
        `lon` DOUBLE, 
        `transaction_datetime` TIMESTAMP_LTZ(3),
        WATERMARK FOR transaction_datetime AS transaction_datetime - INTERVAL '5' SECOND
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
  ```
  - A query de agrupamento deve ser feita de modo que a prioridade seja sempre primeiro a função de agregação e depois a customização das demais colunas.
  ```python
  def insert_stream_s3_grouped(insert_from, insert_into):
    return f"""INSERT INTO {insert_into} 
               SELECT qry.customer, 
                      qry.start_event_time as transaction_datetime,
                      qry.transactions_qtde, 
                      YEAR(qry.start_event_time) as year_rec, 
                      MONTH(qry.start_event_time) as month_rec, 
                      DAYOFMONTH(qry.start_event_time) as day_rec  
               FROM (   
                    SELECT  customer, 
                            transaction_type,
                            TUMBLE_START(transaction_datetime, INTERVAL '60' SECOND ) as start_event_time,  
                            COUNT(*) as transactions_qtde 
                    FROM {insert_from}
                    GROUP BY TUMBLE(transaction_datetime, INTERVAL '60' SECOND ), customer, transaction_type  
               ) qry 
  """
  ```
  - O resultado final gerou o seguinte esquema:
  ![Tumbling Aggregation Diagram](./images/Screenshot%20from%202022-09-21%2015-27-13.png)