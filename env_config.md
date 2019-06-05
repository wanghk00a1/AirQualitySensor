# 环境配置

## 目标

使用实验室的 k8s 服务器，根据需求创建images，构建16台容器组成的Hadoop、Spark、Flink 运行环境。

## 配置内容

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
5. 创建其他tunnel

| |Srk8s机器上执行|Student@cocserver执行
|--|:--|:--|
|NameNode|`ssh -Nf -L localhost:10331:10.244.1.7:50070 root@10.244.1.7` |`ssh -NfL 10331:127.0.0.1:10331 srk8s@202.45.128.243 -p 10846`|
|ResourceManager| `ssh -Nf -L localhost:10332:10.244.1.7:8088 root@10.244.1.7` |`ssh -NfL 10332:127.0.0.1:10332 srk8s@202.45.128.243 -p 10846`|
|MapReducer<br>JobHistory Server|`ssh -Nf -L localhost:10333:10.244.1.7:19888 root@10.244.1.7` |`ssh -NfL 10333:127.0.0.1:10333 srk8s@202.45.128.243 -p 10846`|
|NodeManager|`ssh -Nf -L localhost:10334:10.244.1.7:8042 root@10.244.1.7` | `ssh -NfL 10334:127.0.0.1:10334 srk8s@202.45.128.243 -p 10846`|
|Spark JobHistory|


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

    # 常用docker 命令
    # 查看 images
    sudo docker images

    # 以bash形式进入image
    sudo docker exec -it image_id
    ```

3. 进入主机 `ssh root@10.244.1.7` (root:ubuntu,hduser:student),以此作为后续新的image，基本更新操作：

    ```Shell
    # 更新
    sudo apt-get update
    sudo apt-get upgrade
    sudo apt-get install software-properties-common
    sudo apt-get install aptitude
    sudo apt-get install net-tools
    sudo apt-get install inetutils-ping
    sudo apt-get install tcl tk expect

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

4. 修改 /etc/hosts 文件，注释掉之前的内容，并添加 16台机器的ip映射,每次重新分配pods 会导致ip变化，需要更新文件。

    ```Shell
    10.244.1.7      master
    10.244.7.8      slave01
    10.244.40.7     slave02
    10.244.37.4     slave03
    10.244.30.9     slave04
    10.244.9.7      slave05
    10.244.39.11    slave06
    10.244.34.9     slave07
    10.244.36.6     slave08
    10.244.19.3     slave09
    10.244.3.12     slave10
    10.244.17.7     slave11
    10.244.22.4     slave12
    10.244.8.7      slave13
    10.244.38.9     slave14
    10.244.25.6     slave15

    10.244.1.7      fyp-678d9999b4-xmk7w
    10.244.7.8      fyp-678d9999b4-4sqv5
    10.244.40.7     fyp-678d9999b4-59sjg
    10.244.37.4     fyp-678d9999b4-6nptx
    10.244.30.9     fyp-678d9999b4-8ptqs
    10.244.9.7      fyp-678d9999b4-dmpnq
    10.244.39.11    fyp-678d9999b4-dt9ts
    10.244.34.9     fyp-678d9999b4-fhlqm
    10.244.36.6     fyp-678d9999b4-fm2gb
    10.244.19.3     fyp-678d9999b4-jmbt9
    10.244.3.12     fyp-678d9999b4-lx257
    10.244.17.7     fyp-678d9999b4-n52p8
    10.244.22.4     fyp-678d9999b4-qj5p8
    10.244.8.7      fyp-678d9999b4-sbq4s
    10.244.38.9     fyp-678d9999b4-vldxw
    10.244.25.6     fyp-678d9999b4-xds5s
    ```

5. 修改 hadoop 配置文件，包括：core-site.xml,hdfs-site.xml,yarn-site.xml,mapred-site.xml,slaves,masters 等。
    - 此时本地创建到 **cocserver 10331** 的tunnel: `ssh -L 10331:127.0.0.1:10331 student@202.45.128.135` 即可访问 [HDFS NameNode页面](http://localhost:10331/dfshealth.html#tab-datanode)

6. 如有需要将 master 节点上改过的配置项，可通过 /opt/shell 或 /home/hduser/.sh 中的脚本替换到各个slave 节点里。

#### 基本使用

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

