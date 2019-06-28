package hk.hku.cloud.kafka.controller;

import com.google.gson.Gson;
import hk.hku.cloud.kafka.domain.TweetStatisticEntity;
import hk.hku.cloud.kafka.service.KafkaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * @author: LexKaing
 * @create: 2019-04-01 23:36
 * @description: 目前不处理多页面同时访问导致的竞争情况
 **/
@RestController
@EnableAsync
public class KafkaController {

    private static final Logger logger = LoggerFactory.getLogger(KafkaController.class);

    @Autowired
    KafkaService kafkaService;

    /**
     * MessageMapping和 RequestMapping功能类似
     * 如果服务器接受到了消息，就会对订阅了@SendTo括号中的地址传送消息。
     */
//    @MessageMapping("/initSentiment")
//    @SendTo("/topic/initSentiment")
//    public String initSentiment(String message) {
//        logger.info("receive msg : " + message);
//        // 开启线程处理标志
//        kafkaService.setConsumeKafka(true);
//        // 启动kafka 线程
//        kafkaService.consumeTweets();
//        // 启动心跳
//        kafkaService.keepSocketAlive();
//        return message;
//    }

    /**
     * 开关kafka tweet 的订阅
     */
    @MessageMapping("/updateConsumer")
    public void updateConsumer(String message) {
        if (message.equals("close")) {
            kafkaService.setConsumeKafka(false);
        } else {
            kafkaService.setConsumeKafka(true);
        }
    }

    /**
     * 读取 mysql aqi 数据
     */
    @RequestMapping(value = "/api/getAqiByCity", method = RequestMethod.GET, produces = {"application/json"})
    @ResponseBody
    public String getCurrentAQI(@RequestParam("city") String city, @RequestParam("limit") int limit) {
        List<TweetStatisticEntity> result = kafkaService.getAqiDataByCity(city, limit);
        return new Gson().toJson(result);
    }

}
