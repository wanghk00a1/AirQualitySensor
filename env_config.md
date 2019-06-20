# 环境配置

## 目标

使用实验室的 k8s 服务器，根据需求创建images，构建16台容器组成的Hadoop、Spark、Flink 运行环境。

## 配置内容

#### 常用账号

student => student
srk8s => 123456
root => ubuntu
hduser => student

#### 构建tunneling

1. 以Mac为例，连接 CSVPN，登陆cocserver:
    `ssh student@202.45.128.135`
    可将Mac的~/.ssh/id_rsa.pub 复制到服务器的 ~/.ssh/authorized_keys 中，免密登陆。
2. 建立cocserver 到 k8s 的 tunnel:
    `ssh -NfL 8030:127.0.0.1:8001 srk8s@202.45.128.243 -p 10846`
    cocserver 上执行一次即可。
3. mac 上建立本地到cocserver 的tunnel:
    `ssh -L 8001:127.0.0.1:8030 student@202.45.128.135`
4. 保持步骤3的terminal活跃状态，可以在本地访问 [kubernetes管理页面](http://localhost:8001/api/v1/namespaces/kube-system/services/https:kubernetes-dashboard:/proxy/#!/login)
5. 创建其他tunnel（需要先启动对应服务）

||Srk8s机器上执行|Student@cocserver执行|
|--|:--|:--|
|NameNode|`ssh -Nf -L localhost:10331:10.244.1.7:50070 root@10.244.1.7` |`ssh -NfL 10331:127.0.0.1:10331 srk8s@202.45.128.243 -p 10846`|
|ResourceManager| `ssh -Nf -L localhost:10332:10.244.1.7:8088 root@10.244.1.7` |`ssh -NfL 10332:127.0.0.1:10332 srk8s@202.45.128.243 -p 10846`|
|MapReducer<br>JobHistory Server|`ssh -Nf -L localhost:10333:10.244.1.7:19888 root@10.244.1.7` |`ssh -NfL 10333:127.0.0.1:10333 srk8s@202.45.128.243 -p 10846`|
|NodeManager|`ssh -Nf -L localhost:10334:10.244.1.7:8042 root@10.244.1.7` | `ssh -NfL 10334:127.0.0.1:10334 srk8s@202.45.128.243 -p 10846`|
|Spark JobHistory|`ssh -Nf -L localhost:10335:10.244.1.7:18080 root@10.244.1.7` |`ssh -NfL 10335:127.0.0.1:10335 srk8s@202.45.128.243 -p 10846`|
|CloudWeb|`ssh -Nf -L localhost:10336:10.244.1.7:9999 root@10.244.1.7` |`ssh -NfL 10336:127.0.0.1:10336 srk8s@202.45.128.243 -p 10846`|
|Flink|`ssh -Nf -L localhost:10337:10.244.1.7:8081 root@10.244.1.7` |`ssh -NfL 10337:127.0.0.1:10337 srk8s@202.45.128.243 -p 10846`|

#### 构建镜像

1. 通过cocserver 登陆 k8s 服务器, pwd:123456
   `ssh srk8s@202.45.128.243 -p 10846`
2. 切换到 zsh 方便查看当前 NameSpace.

    ```Shell
    # 常用k8s 命令
    # 切换 NameSpace
    kubectl config set-context $(kubectl config current-context) --namespace=msc-group03
    # 查看当前 pods
    kubectl get pods -o wide
    ```

3. 根据image 创建container：
   - `sudo docker images` 查看docker镜像，选择一个，
   - create container from an image : `sudo docker run -dit image_name:Tag`
   - 通过`sudo docker container ls` 找到刚刚创建的container
   - 进入container : `sudo docker exec -it container_id bash`
   - 配置execution environment

4. 根据container 创建image
   - after creating execution environment, create image: `sudo docker commit --author "msc-group03" --message "create image 0605" container_id repository_name`

#### 配置execution environment

1. 进入 docker container 后，执行基本更新操作：

    ```Shell
    # 更新
    sudo apt-get update
    sudo apt-get upgrade
    sudo apt-get install software-properties-common
    sudo apt-get install aptitude
    sudo apt-get install net-tools
    sudo apt-get install inetutils-ping
    sudo apt-get install tcl tk expect
    sudo apt-get --reinstall install language-pack-en

    # 安装 zsh  -- 需要和嘉星再对一下
    sudo curl -fL https://gist.githubusercontent.com/tsengkasing/5f0e89e8eb14aac4d8760ac35156eb53/raw/0f66fddfc18efb1045f0d1f7ad8859d38b966bed/install-zsh.sh | bash
    # 更换sh
    chsh -s /bin/bash
    chsh -s /bin/zsh

    # 已安装 java, 已创建hduser/hadoop 用户/组
    # Java 8 is no longer available to download publically. You can install Java 11.
    # https://tecadmin.net/install-oracle-java-11-ubuntu-18-04-bionic/

    # 解决奇妙问题
    ```

2. 修改 /etc/hosts 文件，注释掉localhost，并添加16台机器的ip映射,每次重新分配pods 会导致ip变化，需要生成pods后再更新master节点文件，并通过脚本拷贝到slaves上。

    ```Shell
    10.244.1.7      master
    10.244.1.7      fyp-678d9999b4-xmk7w
    ...
    ```

3. 修改 hadoop 配置文件，包括：core-site.xml,hdfs-site.xml,yarn-site.xml,mapred-site.xml,slaves,masters 等。
    - 此时本地创建到 **cocserver 10331** 的tunnel: `ssh -L 10331:127.0.0.1:10331 student@202.45.128.135` 即可访问 [HDFS NameNode页面](http://localhost:10331/dfshealth.html#tab-datanode)

4. 如有需要将 master 节点上改过的配置项，可通过 /opt/shell 或 /home/hduser/.sh 中的脚本替换到各个slave 节点里。

#### 页面访问

1. 访问 k8s 页面需要:
   `ssh -L 8001:127.0.0.1:8030 student@202.45.128.135`
2. 访问 NameNode 页面需要:
   `ssh -L 10331:127.0.0.1:10331 student@202.45.128.135`
3. 访问 ResourceManager 页面需要:
   `ssh -L 10332:127.0.0.1:10332 student@202.45.128.135`
4. 访问 MapReducer(JobHistory) 页面需要:
   `ssh -L 10333:127.0.0.1:10333 student@202.45.128.135`
5. 访问 NodeManager 页面需要:
    `ssh -L 10334:127.0.0.1:10334 student@202.45.128.135`
6. 访问 Spark History 页面需要先启动Spark history:
    `/opt/spark-2.4.3-bin-hadoop2.7/sbin/start-history-server.sh`
    再构建本地tunnel:
    `ssh -L 10335:127.0.0.1:10335 student@202.45.128.135`
7. 访问 CloudWeb 页面需要:
    `ssh -L 10336:127.0.0.1:10336 student@202.45.128.135`
8. 访问 Flink 页面需要:
    `ssh -L 10337:127.0.0.1:10337 student@202.45.128.135`

#### 基本使用

1. [Kafka 使用](https://gist.github.com/AlexTK2012/7a1c68ec2b904528c41e726ebece4b46)

2. [Flume 使用](https://gist.github.com/AlexTK2012/1d3288f0e474b4ad66db80950b402230)

3. 启动flume:
`nohup flume-ng agent -f /home/hduser/app/AirQualitySensor/Collector/TwitterToKafka.conf -Dflume.root.logger=DEBUG,console -n a1 >> flume.log 2>&1 &`

4. Spark Streaming 训练Naive Bayes模型: `spark-submit --class "hk.hku.spark.mllib.SparkNaiveBayesModelCreator" --master local /home/hduser/app/AirQualitySensor/StreamProcessorSpark/target/StreamProcessorSpark-jar-with-dependencies.jar`

5. 启动spark streaming 读取kafka twitter 数据
`spark-submit --class "hk.hku.spark.TweetSentimentAnalyzer" --master yarn --deploy-mode client --num-executors 8 --executor-memory 4g --executor-cores 4 --driver-memory 4g --conf spark.yarn.executor.memoryOverhead=2048 /home/hduser/app/AirQualitySensor/StreamProcessorSpark/target/StreamProcessorSpark-jar-with-dependencies.jar`

6. 启动cloudweb: `nohup java -jar /home/hduser/app/AirQualitySensor/CloudWeb/target/CloudWeb-1.0-SNAPSHOT.jar`

7. 启动/关闭 Flink 集群: /opt/flink-1.7.2/bin/start-cluster.sh  stop-cluster.sh

8. 启动 flink 任务: `flink run -c hk.hku.flink.TweetFlinkAnalyzer StreamProcessorFlink-jar-with-dependencies.jar` , 之后在 Flink 管理页面查看任务执行情况。[Flink 基本命令](https://blog.csdn.net/sunnyyoona/article/details/78316406)

