package hk.hku.flink.process;

import hk.hku.flink.corenlp.CoreNLPSentimentAnalyzer;
import hk.hku.flink.utils.Constants;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.GeoLocation;
import twitter4j.Status;
import twitter4j.TwitterObjectFactory;

import static hk.hku.flink.utils.Constants.COMMA;

/**
 * @author: LexKaing
 * @create: 2019-06-24 15:01
 * @description: flink run -c hk.hku.flink.process.PreProcess StreamProcessorFlink-jar-with-dependencies.jar
 **/
public class PreProcess {
    private static final Logger logger = LoggerFactory.getLogger(PreProcess.class);

    public static void main(String[] args) {
        logger.info("Tweets PreProcess job start");
        PreProcess preProcess = new PreProcess();
        preProcess.startJob();
    }

    private void startJob() {
        String inputFile = "/tweets/data-bak0621/twitter.log";
        String outputFIle = "/tweets/flink/twitter.csv";

        // 创建运行环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度15 台机器
        env.setParallelism(15);


        DataSet<String> originFile = env
                .readTextFile(inputFile)
                .name("readHDFS");

        DataSet<Status> filteredFile = originFile.map(line -> TwitterObjectFactory.createStatus(line))
                .filter(line -> line.getGeoLocation() == null && line.getPlace() == null);


//            System.out.println(filteredFile.count());

        DataSet<String> processData = filteredFile
                .map(new TweetPraseMap())
                .name("process data");


        processData
                .writeAsText(outputFIle, FileSystem.WriteMode.OVERWRITE)
                .name("write to hdfs");

        try {
            env.execute("read from hdfs");
        } catch (Exception e) {
            logger.error("Exception", e);
        }

    }


}