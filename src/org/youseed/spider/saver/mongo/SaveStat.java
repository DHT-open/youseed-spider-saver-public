package org.youseed.spider.saver.mongo;

import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.youseed.spider.MongoConn;
import org.youseed.spider.RabbitMQConn;
import org.youseed.spider.SpiderConfig;
import org.youseed.spider.saver.MongoBasic;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * 保存爬虫统计信息
 */
public class SaveStat extends MongoBasic {

	/**
	 * 消费者标签
	 */
	static final String CONSUME_TAG = "mongo-stat-consumer";

	private static Logger logger = LogManager.getLogger(SaveStat.class);

	RabbitMQConn mq = new RabbitMQConn();

	MongoConn mongo = new MongoConn();

	public SaveStat() {
		super();
	}
	
	/**
	 * 处理Hash
	 */
	public void consume() throws IOException {

		//监听消息
		Channel channel = mq.getChannel();
		channel.queueDeclare(mqMongoStatQueue, true, false, false, null);
		channel.queueBind(mqMongoStatQueue, mqMongoExchange, mqMongoStatRouting);
		channel.basicConsume(mqMongoStatQueue, false, CONSUME_TAG, new DefaultConsumer(channel) {

			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
					byte[] body) {
				
				long deliveryTag = envelope.getDeliveryTag();

				//1解析数据
				JSONObject data = null;
				try {
					data = JSON.parseObject(new String(body, "UTF-8"));
					
					String dateStr = data.getString("date");
					data.put("date", new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse(dateStr));
					logger.info("已保存，爬虫" + data.getString("spider") + "|" + dateStr);
				} catch (Exception e) {
					logger.error("JSON解析失败，提交成功并跳过消息：" + e.getMessage(), e);
					confirmMsg(channel, deliveryTag);
					return;
				}
				
				//2保存数据
				try {
					mongo.save(SpiderConfig.COLL_STATE, new Document(data));
				} catch (Exception e) {
					logger.error("保存失败：" + e.getMessage(), e);
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
		SaveStat main = new SaveStat();
		main.consume();
	}
	
}
