package hk.hku.cloud;

import com.google.gson.Gson;
import hk.hku.cloud.kafka.controller.KafkaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.socket.config.annotation.EnableWebSocket;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;
import java.util.Properties;

//@SpringBootApplication same as @Configuration @EnableAutoConfiguration @ComponentScan 
@EnableAutoConfiguration
@ServletComponentScan
@ComponentScan
@Configuration
@EnableConfigurationProperties
// @EnableTransactionManagement //开启事务管理
@EnableScheduling
@EnableWebSocket
public class Application {
	private static final Logger logger = LoggerFactory.getLogger(Application.class);

	//spring boot 启动类
	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
		logger.info("CloudWeb start ! ");
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
