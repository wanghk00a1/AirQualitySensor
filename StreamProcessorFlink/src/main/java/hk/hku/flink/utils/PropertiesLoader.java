package hk.hku.flink.utils;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * @author: LexKaing
 * @create: 2019-06-12 17:54
 * @description:
 **/
public class PropertiesLoader {

    private static Config conf = ConfigFactory.load("application.conf");

    // naive bayes 参数
    public static String sentiment140TrainingFilePath = conf.getString("SENTIMENT140_TRAIN_DATA_ABSOLUTE_PATH");
    public static String sentiment140TestingFilePath = conf.getString("SENTIMENT140_TEST_DATA_ABSOLUTE_PATH");
    public static String nltkStopWords = conf.getString("NLTK_STOPWORDS_FILE_NAME ");
    public static String naiveBayesModelPath = conf.getString("NAIVEBAYES_MODEL_ABSOLUTE_PATH");

    // core nlp 的模型都在maven依赖里，暂无需要配置的参数

    // consume kafka stream 数据
    public static String bootstrapServers = conf.getString("BOOTSTRAP_SERVER");
    public static String groupId = conf.getString("GROUP_ID");
    public static String autoOffsetReset = conf.getString("AUTO_OFFSET_RESET");
    public static String topicConsumer = conf.getString("KAFKA_TOPIC_CONSUMER");

    // produce kafka 吐结果数据
    public static String bootstrapServersProducer = conf.getString("BOOTSTRAP_SERVER_PRODUCER");
    public static String groupIdProducer = conf.getString("GROUP_ID_PRODUCER");
    public static String topicProducer = conf.getString("KAFKA_TOPICS_PRODUCER");

    // weather keywords
    public static String weatherKeywords = conf.getString("WEATHER_KEYWORD");

    public static void main(String[] args) {
        System.out.println(PropertiesLoader.sentiment140TestingFilePath);
    }

}