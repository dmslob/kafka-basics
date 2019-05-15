## Kafka Example
## Getting Started

### For starting Windows OS cmd line
    Being in C:\kafka_2.11-2.1.0
    zookeeper-server-start.bat config/zookeeper.properties
    kafka-server-start.bat config/server.properties

    After run application
    kafka-topics --zookeeper 127.0.0.1:2181 --topic first_topic --describe
    kafka-console-consumer --bootstrap-server 127.0.0.1:9091 --topic first_topic --from-beginning

    To create a topic
    kafka-topics --zookeeper 127.0.0.1:2181 --create --topic twitter_tweets --partitions 6 --replication-factor 1

    To start Elasticsearch
    C:\elasticsearch-7.0.1\bin\elasticsearch.bat
    GET http://localhost:9200/
    GET http://localhost:9200/_cat/nodes?v
    GET http://localhost:9200/_cat/indices?v

    To create index
    PUT http://localhost:9200/twitter

    PUT http://localhost:9200/twitter/tweets/1
    BODY:
    {
    	"app": "kafka-twitter",
    	"author": "dmslob"
    }
    And to get
    GET http://localhost:9200/twitter/tweets/1