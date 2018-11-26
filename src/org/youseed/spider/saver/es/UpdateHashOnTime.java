package org.youseed.spider.saver.es;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

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
 * 更新Hash热度(用于定时任务调用，例如每天执行一次更新，完毕后即退出)
 */
public class UpdateHashOnTime extends ESBasic {

	/**
	 * 消费者标签
	 */
	static final String CONSUME_TAG = "es-update-consumer";
	
	private static Logger logger = LogManager.getLogger(UpdateHashOnTime.class);

	RabbitMQConn mq = new RabbitMQConn();
	ESConn es  = new ESConn();
	
	/**
	 * 构造函数
	 */
	public UpdateHashOnTime() {
		super();
		logger.info("starting update hash(on time) consumer...");
	}

	/**
	 * 更新Hash
	 */
	public void consume() throws IOException {
		TransportClient client = es.getClient();
		Channel channel = mq.getChannel();
		
		channel.queueDeclare(mqEsUpdateQueue, true, false, false, null);
		channel.queueBind(mqEsUpdateQueue, mqEsExchange, mqEsUpdateRouting);
		
		final long count = channel.messageCount(mqEsUpdateQueue);
		logger.info("待更新Hash批次数：" + count);
		
		channel.basicConsume(mqEsUpdateQueue, false, CONSUME_TAG, new DefaultConsumer(channel) {
			
			//需执行的批次
			long batch = count;

			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
				
				//判断此批次是否已经处理完成
				if(batch <= 0) {
					try {
						channel.close();
						logger.info("批次任务已经更新完毕，程序退出。");
						return;
					} catch (IOException | TimeoutException e) {
					}
					
				}else {
					logger.info("剩余更新批次：" + batch);
					batch--;
				}
				
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
		UpdateHashOnTime main = new UpdateHashOnTime();
		main.consume();
	}
}
