package hk.hku.cloud.config;

import hk.hku.cloud.kafka.service.KafkaService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Properties;

/**
 * @author: LexKaing
 * @create: 2019-06-28 21:07
 * @description:
 **/
@Component
@EnableAsync
public class MyApplicationRunner implements ApplicationRunner {

    @Autowired
    KafkaService kafkaService;

    @Override
    public void run(ApplicationArguments args) {
        System.out.println("MyCommandLineRunner...");

        kafkaService.consumeTweets();
        kafkaService.consumeStatistic();

        System.out.println("MyCommandLineRunner kafka service started...");
    }

    @Deprecated
    private static void test() {
        try {
            Properties props = System.getProperties();
            System.out.println("用户的当前工作目录：    " + props.getProperty("user.dir"));

            FileReader fr = new FileReader("./CloudWeb/src/main/resources/test.txt");

            BufferedReader bufr = new BufferedReader(fr);
            String line;
            String DELIMITER = "¦";
            while ((line = bufr.readLine()) != null) {
                String[] strArray = line.split(DELIMITER);
                String mlSentiment = strArray[4].equals("1") ? "positive" : (strArray[4].equals("0") ? "neutral" : "negative");
                String nlpSentiment = strArray[3].equals("1") ? "positive" : (strArray[3].equals("0") ? "neutral" : "negative");
                System.out.println("id : " + strArray[0] + ",mllib : " + mlSentiment + " ,nlp : " + nlpSentiment);
            }

            bufr.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}