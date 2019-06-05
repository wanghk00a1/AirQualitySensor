# Social Media as a Sensor of Air Quality and Public Response

[![](https://img.shields.io/badge/hadoop-v2.7.5-brightgreen.svg?style=flat-square)](https://hadoop.apache.org) [![](https://img.shields.io/badge/spark-v2.4.0-brightgreen.svg?style=flat-square)](https://spark.apache.org) [![](https://img.shields.io/badge/Flume-v1.9.0-brightgreen.svg?style=flat-square)](https://flume.apache.org) [![](https://img.shields.io/badge/Kafka-v2.1.1-brightgreen.svg?style=flat-square)](http://kafka.apache.org) [![](https://img.shields.io/badge/Flink-v1.7.2-brightgreen.svg?style=flat-square)](https://flink.apache.org)  [![](https://img.shields.io/badge/Scala-v2.11.12-brightgreen.svg?style=flat-square)](https://www.scala-lang.org) [![](https://img.shields.io/badge/Python-v3.6.7-brightgreen.svg?style=flat-square)](https://www.python.org)

Developed By:

  - [@lexkaing](https://github.com/AlexTK2012/)
  - [@WangHanke](https://github.com/wanghk00a1/)
  - [@Everstar](https://github.com/tsengkasing/)

## :rocket: Getting Started

### :package: Dependencies Installation

Please Use Maven to manage the dependencies.

```shell
$ cd StreamProcessorSpark
$ mvn package
```

### :beer: Runs

- Pack the Jar file
- Upload it to Machine GPU5
    ```shell
    $ scp -o ProxyCommand='ssh group05@202.45.128.135 -W %h:%p' AirQualitySensor.jar hduser@gpu5:~/app/temp/
    ```
- Execute jar file
    ```shell
    $ ssh ...
    $ cd ~/app/temp
    $ spark-submit --class AirQualitySensor.Analyzer ./AirQualitySensor.jar test04
    ```
- Start Kafka Producer (Option 1)
    ```shell
    $ kafka-console-producer.sh --broker-list gpu5:9092 --topic test04
    $> input your text here
    ```
- Start Flume to migrade data to Kafka
    > Configuration file hardcoded topic "test04"
    ```shell
    $ flume-ng agent -c /home/hduser/app/flume-rate-controller -f rate-control-flume-conf.properties --name agent -Dflume.root.logger=INFO,console
    ```
    PS: The **test04** above is the **topic** of *Kafka* , you can custom it by creating a new topic.

## :sparkles: Project Structure

```
.
├── CloudWeb
├── Collector
├── HBaser
├── StreamProcessorFlink
├── StreamProcessorSpark
|
└── pom.xml(Maven parent POM)

```
 - __CloudWeb__: 
 - __Collector__:
   - Collect data from Twitter
 - __StreamProcessorFlink__:
 - __StreamProcessorSpark__:


## :memo: Reference Git Repository

- [Spark-MLlib-Twitter-Sentiment-Analysis](https://github.com/P7h/Spark-MLlib-Twitter-Sentiment-Analysis)

- [flume\_kafka\_spark\_solr\_hive](https://github.com/obaidcuet/flume_kafka_spark_solr_hive/tree/master/codes) 

- [corenlp-scala-examples](https://github.com/harpribot/corenlp-scala-examples)

## Data

Collect AQI Data from [Air Visual](https://www.airvisual.com/usa/california/san-francisco)