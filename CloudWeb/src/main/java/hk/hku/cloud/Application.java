package hk.hku.cloud;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

//@SpringBootApplication same as @Configuration @EnableAutoConfiguration @ComponentScan 
@EnableAutoConfiguration
@ServletComponentScan
@ComponentScan
@Configuration
@EnableConfigurationProperties
// @EnableTransactionManagement //开启事务管理
@EnableScheduling
public class Application {
	private static final Logger logger = LoggerFactory.getLogger(Application.class);

	//spring boot 启动类
	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
		logger.info("CloudWeb start ! ");
		logger.info("test");
	}
}
