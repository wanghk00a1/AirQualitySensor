package hk.hku.flink.process;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author: LexKaing
 * @create: 2019-06-24 15:01
 * @description:
 **/
public class PreProcess {
    private static final Logger logger = LoggerFactory.getLogger(PreProcess.class);

    public static void main(String[] args) {
        logger.info("Tweets Flink Analyzer job start");
        PreProcess preProcess = new PreProcess();
        preProcess.startJob();
    }

    private void startJob() {
        // 创建运行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 启用容错 checkpoint every 5000 msecs
        env.enableCheckpointing(5000);
        // 设置并行度15 台机器
        env.setParallelism(2);
    }



}