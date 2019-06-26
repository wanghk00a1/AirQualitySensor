package hk.hku.cloud.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.scheduling.concurrent.DefaultManagedTaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

/**
 * @author: LexKaing
 * @create: 2019-04-01 23:33
 * @description:
 * @EnableWebSocketMessageBroker 开启使用STOMP协议来传输基于代理的消息，Broker就是代理的意思。
 **/

@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    /**
     * 注册STOMP协议的节点，并指定映射的URL，供客户端与服务器端建立连接
     *
     * @param registry
     */
    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        System.out.println("registerStompEndpoints");
        // 注册STOMP协议节点，同时指定使用SockJS协议
        registry.addEndpoint("/endpointSang").withSockJS();
    }

    /**
     * 配置消息代理，实现推送功能，这里的消息代理是/topic
     * 服务端发送消息给客户端的域,多个用逗号隔开
     *
     * @param registry
     */
    @Override
    public void configureMessageBroker(MessageBrokerRegistry registry) {
        System.out.println("configureMessageBroker");

//        // 自定义调度器，用于控制心跳线程
//        ThreadPoolTaskScheduler taskScheduler = new ThreadPoolTaskScheduler();
//        // 线程池线程数，心跳连接开线程
//        taskScheduler.setPoolSize(1);
//        // 线程名前缀
//        taskScheduler.setThreadNamePrefix("websocket-heartbeat-thread-");
//        // 初始化
//        taskScheduler.initialize();

        /*
         * spring 内置broker对象
         * 1. 配置代理域，可以配置多个，此段代码配置代理目的地的前缀为 /topic, 可以在配置的域上向客户端推送消息
         * 2，进行心跳设置，第一值表示server最小能保证发的心跳间隔毫秒数, 第二个值代码server希望client发的心跳间隔毫秒数
         * 3. 可以配置心跳线程调度器 setHeartbeatValue这个不能单独设置，要配合setTaskScheduler才可以生效
         *    调度器也可以使用默认的调度器 new DefaultManagedTaskScheduler()
         */
        registry.enableSimpleBroker("/topic")
                .setHeartbeatValue(new long[]{5000, 5000})
//                .setTaskScheduler(taskScheduler);
                .setTaskScheduler(new DefaultManagedTaskScheduler());

    }

}