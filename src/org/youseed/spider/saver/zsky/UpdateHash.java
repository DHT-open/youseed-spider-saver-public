package org.youseed.spider.saver.zsky;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rex.DB;
import org.rex.db.Ps;
import org.youseed.spider.MysqlConn;
import org.youseed.spider.RabbitMQConn;
import org.youseed.spider.saver.MysqlBasic;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * 更新将消息队列中的Hash
 */
public class UpdateHash extends MysqlBasic {

	/**
	 * 消费者标签
	 */
	static final String CONSUME_TAG = "mysql-zsky-update-consumer";

	private static Logger logger = LogManager.getLogger(UpdateHash.class);

	RabbitMQConn mq = new RabbitMQConn();
	
	public UpdateHash() {
		super();
	}
	
	final static String UPDATE_HASH = "UPDATE search_hash SET last_seen=now(),requests=requests+1 WHERE info_hash like ?";

	/**
	 * 更新Hash
	 */
	public void consume() throws IOException {
		
		//初始化数据库连接
		MysqlConn.initDB();

		//监听消息
		Channel channel = mq.getChannel();
		channel.queueDeclare(mqMysqlUpdateQueue, true, false, false, null);
		channel.queueBind(mqMysqlUpdateQueue, mqMysqlExchange, mqMysqlUpdateRouting);
		channel.basicConsume(mqMysqlUpdateQueue, false, CONSUME_TAG, new DefaultConsumer(channel) {

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
					List<Ps> pss = new ArrayList<Ps>();
					for (int i = 0; i < data.size(); i++) {
						pss.add(new Ps().add(data.get(i) + "%"));
					}
					
					int[] cnts = DB.batchUpdate(UPDATE_HASH, pss);
					
					int n = 0;
					for (int i = 0; i < cnts.length; i++) {
						n += cnts[i];
					}
					logger.info("实际更新：" + n);
				}catch(Exception e) {
					logger.error("更新失败：" + e.getMessage(), e);
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
		UpdateHash main = new UpdateHash();
		main.consume();
	}
}
