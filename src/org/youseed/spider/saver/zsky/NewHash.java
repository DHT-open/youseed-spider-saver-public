package org.youseed.spider.saver.zsky;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rex.DB;
import org.rex.db.Ps;
import org.rex.db.exception.DBException;
import org.youseed.spider.MysqlConn;
import org.youseed.spider.RabbitMQConn;
import org.youseed.spider.saver.MysqlBasic;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import io.netty.util.internal.StringUtil;

/**
 * 保存将消息队列中的新Hash
 * XXX:未使用批量写入，没有丢数据风险，但是性能比较差
 */
public class NewHash extends MysqlBasic {

	/**
	 * 消费者标签
	 */
	static final String CONSUME_TAG = "mysql-zsky-new-consumer";

	private static Logger logger = LogManager.getLogger(NewHash.class);

	RabbitMQConn mq = new RabbitMQConn();

	public NewHash() {
		super();
	}
	
	//新hash
	final static String INSERT_HASH = "INSERT IGNORE INTO search_hash(info_hash, category, data_hash, name, extension, source_ip, length, create_time, last_seen, requests) "
			+ "VALUES (?,?,?,?,?,?,?,now(),now(),1)";	
	//新filelist
	final static String INSERT_FILELIST = "INSERT IGNORE INTO search_filelist(info_hash, file_list) VALUES (?,?)";
	
	/**
	 * 处理Hash
	 */
	public void consume() throws IOException {
		
		//初始化数据库连接
		MysqlConn.initDB();
		
		//监听消息
		Channel channel = mq.getChannel();
		channel.queueDeclare(mqMysqlNewQueue, true, false, false, null);
		channel.queueBind(mqMysqlNewQueue, mqMysqlExchange, mqMysqlNewRouting);
		channel.basicConsume(mqMysqlNewQueue, false, CONSUME_TAG, new DefaultConsumer(channel) {

			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
					byte[] body) {

				long deliveryTag = envelope.getDeliveryTag();
				
				//1.解析数据
				JSONObject data = null;
				try {
					data = JSON.parseObject(new String(body, "UTF-8"));
				} catch (Exception e) {
					logger.error("JSON解析失败，提交成功并跳过消息：" + e.getMessage(), e);
					confirmMsg(channel, deliveryTag);
					return;
				}
				
				//2.保存数据
				//2.1格式校验
				String infoHash = data.getString("info_hash");
				String name = data.getString("name");
				if(StringUtil.isNullOrEmpty(infoHash) || StringUtil.isNullOrEmpty(name)) {
					logger.info("数据格式不正确，跳过消息");
					confirmMsg(channel, deliveryTag);
					return;
				}

				logger.info("新资源: " + infoHash);
				
				// 3保存数据
				try {
					//2.2hash
					DB.update(INSERT_HASH,new Ps(
							infoHash,
							data.getString("category"),
							data.getString("data_hash"),
							data.getString("name"),
							data.getString("extension") == null ? null : data.getString("extension").trim(),
							data.getString("source_ip"),
							data.getLong("length")));
					
					//2.3files
					JSONArray filelist = data.getJSONArray("filelist");
					if (filelist != null) {
						DB.update(INSERT_FILELIST, new Ps(infoHash, filelist.toJSONString()));
					}
					
				} catch (Exception e) {
					logger.error("保存失败：" + e.getMessage(), e);
					testMysqlConn(channel, deliveryTag);
					return;
				}

				//4确认消息
				confirmMsg(channel, deliveryTag);
			}
			
		});
	}

	//test
	public static void main(String[] args) throws IOException, DBException {
		NewHash main = new NewHash();
		main.consume();
	}
}
