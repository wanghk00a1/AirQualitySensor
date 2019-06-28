package hk.hku.cloud.config;

import hk.hku.cloud.kafka.service.KafkaService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Component;


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
}