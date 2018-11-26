package org.youseed.spider.saver.mongo;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.youseed.spider.MongoConn;
import org.youseed.spider.RabbitMQConn;
import org.youseed.spider.saver.MongoBasic;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * 更新将消息队列中的Hash
 */
public class UpdateHash extends MongoBasic {

	/**
	 * 消费者标签
	 */
	static final String CONSUME_TAG = "mongo-update-consumer";

	private static Logger logger = LogManager.getLogger(UpdateHash.class);

	RabbitMQConn mq = new RabbitMQConn();

	MongoConn mongo = new MongoConn();
	
	public UpdateHash() {
		super();
	}

	/**
	 * 更新Hash
	 */
	public void consume() throws IOException {

		//监听消息
		Channel channel = mq.getChannel();
		channel.queueDeclare(mqMongoUpdateQueue, true, false, false, null);
		channel.queueBind(mqMongoUpdateQueue, mqMongoExchange, mqMongoUpdateRouting);
		channel.basicConsume(mqMongoUpdateQueue, false, CONSUME_TAG, new DefaultConsumer(channel) {

			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
					byte[] body) {
				
				long deliveryTag = envelope.getDeliveryTag();

				//1解析数据
				JSONArray data = null;
				try {
					data = JSON.parseArray(new String(body, "UTF-8"));
				} catch (Exception e) {
					logger.error("JSON解析失败，提交成功并跳过消息：" + e.getMessage(), e);
					confirmMsg(channel, deliveryTag);
					return;
				}
				
				//2更新数据
				//2.1解析数据
				int size = data.size();
				if(size == 0) {
					logger.info("空数据，跳过");
					confirmMsg(channel, deliveryTag);
					return;
				}

				logger.info("待更新：" + size);
				
				//2.2更新
				try {
					int cnt = bulkUpdate(mongo, data);
					logger.info("实际更新：" + cnt);
				}catch(Exception e) {
					logger.error("更新失败：" + e.getMessage(), e);
					testMongoConn(mongo, channel, deliveryTag);
					return;
				}

				//3确认消息
				confirmMsg(channel, deliveryTag);
			}
		});
	}

	//test
	public static void main(String[] args) throws IOException {
		UpdateHash main = new UpdateHash();
		main.consume();
	}
}
