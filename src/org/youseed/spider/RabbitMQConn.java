package org.youseed.spider;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.alibaba.fastjson.JSONObject;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * MQ连接
 */
public class RabbitMQConn {

	private static Logger logger = LogManager.getLogger(RabbitMQConn.class);
	
	public Channel getChannel() {
		JSONObject config  = ConfigUtil.getConfig().getJSONObject("mq");
		
		String host = config.getString("host");
		int port = config.getIntValue("port");
		String username = config.getString("username");
		String password = config.getString("password");
		String virtualHost = config.getString("virtualHost");
		
		logger.info("---------RabbitMQ配置------------");
		logger.info("地址|mq.url: " + host);
		logger.info("端口|mq.port: " + port);
		logger.info("账户|mq.username: " + username);
		logger.info("虚拟目录|mq.virtualHost: " + virtualHost);
		logger.info("---------------------------------------------");
		
		
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(host);
		factory.setPort(port);
		factory.setUsername(username);
		factory.setPassword(password);
		factory.setVirtualHost(virtualHost);
		
		Channel channel = null;
		try {
			Connection connection = factory.newConnection();
			channel = connection.createChannel();
		} catch (IOException | TimeoutException e) {
			
		}
		
		return channel;
	}
}
