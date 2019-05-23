#!/bin/bash
# 启动flume 读取twitter数据
nohup flume-ng agent -f /opt/AirQualitySensor/Collector/TwitterToKafka.conf -Dflume.root.logger=DEBUG,console -n a1  >> /opt/AirQualitySensor/Collector/flume.log 2>&1 &


