package org.youseed.spider.saver;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.rabbitmq.client.Channel;

/**
 * 操作MQ
 */
public class MQBasic {

	private static Logger logger = LogManager.getLogger(MQBasic.class);
	
	/**
	 * 确认消息
	 */
	public void confirmMsg(Channel channel, long deliveryTag) {
		try {
			channel.basicAck(deliveryTag, false);
		} catch (IOException e) {
			logger.error("消息确认失败：" + e.getMessage(), e);
		}
	}
	
	/**
	 * 退回消息
	 */
	public void rejectMsg(Channel channel, long deliveryTag) {
		try {
			channel.basicReject(deliveryTag, true);
		} catch (IOException e1) {
			logger.error("消息回退失败：" + e1.getMessage(), e1);
		}
	}
}
