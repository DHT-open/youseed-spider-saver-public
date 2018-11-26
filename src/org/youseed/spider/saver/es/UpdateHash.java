package org.youseed.spider.saver.es;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.transport.TransportClient;
import org.youseed.spider.ESConn;
import org.youseed.spider.RabbitMQConn;
import org.youseed.spider.saver.ESBasic;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * 更新Hash热度（用于不间断监听更新）
 */
public class UpdateHash extends ESBasic {

	/**
	 * 消费者标签
	 */
	static final String CONSUME_TAG = "es-update-consumer";
	
	private static Logger logger = LogManager.getLogger(UpdateHash.class);

	RabbitMQConn mq = new RabbitMQConn();
	ESConn es  = new ESConn();
	
	/**
	 * 构造函数
	 */
	public UpdateHash() {
		super();
		logger.info("starting update hash consumer...");
	}

	/**
	 * 更新Hash
	 */
	public void consume() throws IOException {
		
		TransportClient client = es.getClient();
		Channel channel = mq.getChannel();
		
		channel.queueDeclare(mqEsUpdateQueue, true, false, false, null);
		channel.queueBind(mqEsUpdateQueue, mqEsExchange, mqEsUpdateRouting);
		channel.basicConsume(mqEsUpdateQueue, false, CONSUME_TAG, new DefaultConsumer(channel) {

			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
				
				logger.info("---------------------");
				long deliveryTag = envelope.getDeliveryTag();
				
				//1解析数据
				JSONArray data = null;
				try {
					data = JSON.parseArray(new String(body, "UTF-8"));
					logger.info("获取到数据，条目: " + data.size());
				} catch (Exception e) {
					logger.error("JSON解析失败，提交成功并跳过消息：" + e.getMessage(), e);
					confirmMsg(channel, deliveryTag);
					return;
				}
				
				//2提交处理
				try {
					int cnt = batchUpdateHash(client, data);
					logger.info("更新条目: " + cnt);
				}catch(Exception e) {
					logger.error("更新出错: " + e.getMessage(), e);
					testEsConn(client, channel, deliveryTag);
					return;
				}

				//3确认消息
				confirmMsg(channel, deliveryTag);
			}
		});
	}
	

	/**
	 * 测试
	 */
	public static void main(String[] args) throws IOException {
		UpdateHash main = new UpdateHash();
		main.consume();
	}
}
