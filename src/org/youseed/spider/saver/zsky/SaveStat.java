package org.youseed.spider.saver.zsky;

import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rex.DB;
import org.rex.db.Ps;
import org.youseed.spider.MongoConn;
import org.youseed.spider.RabbitMQConn;
import org.youseed.spider.saver.MysqlBasic;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * 保存爬虫统计信息
 */
public class SaveStat extends MysqlBasic {

	/**
	 * 消费者标签
	 */
	static final String CONSUME_TAG = "mysql-zsky-stat-consumer";

	private static Logger logger = LogManager.getLogger(SaveStat.class);

	RabbitMQConn mq = new RabbitMQConn();

	MongoConn mongo = new MongoConn();
	
	static final String INSERT_REPORT = "INSERT INTO search_statusreport(date,new_hashes,total_requests, valid_requests) VALUES(?,?,?,?)";

	public SaveStat() {
		super();
	}
	
	/**
	 * 处理Hash
	 */
	public void consume() throws IOException {

		//监听消息
		Channel channel = mq.getChannel();
		channel.queueDeclare(mqMysqlStatQueue, true, false, false, null);
		channel.queueBind(mqMysqlStatQueue, mqMysqlExchange, mqMysqlStatRouting);
		channel.basicConsume(mqMysqlStatQueue, false, CONSUME_TAG, new DefaultConsumer(channel) {

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
					logger.info("爬虫" + data.getString("spider") + "|" + dateStr);
				} catch (Exception e) {
					logger.error("JSON解析失败，提交成功并跳过消息：" + e.getMessage(), e);
					confirmMsg(channel, deliveryTag);
					return;
				}
				
				//2保存数据
				try {
					int newHash = data.getIntValue("num_new");
					int total = data.containsKey("total") ? data.getIntValue("total") : 0; 
					int valid =  data.getIntValue("num_new") + data.getIntValue("num_stored");
					
					DB.update(INSERT_REPORT, new Ps(data.get("date"), newHash, total, valid));
					
				} catch (Exception e) {
					logger.error("保存失败：" + e.getMessage(), e);
					testMysqlConn(channel, deliveryTag);
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
