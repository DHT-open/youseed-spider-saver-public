package org.youseed.spider.saver.mongo;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.youseed.spider.MongoConn;
import org.youseed.spider.RabbitMQConn;
import org.youseed.spider.SpiderConfig;
import org.youseed.spider.saver.MongoBasic;
import org.youseed.spider.saver.SpamAnalyzer;

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
 */
public class NewHash extends MongoBasic {

	/**
	 * 消费者标签
	 */
	static final String CONSUME_TAG = "mongo-new-consumer";

	private static Logger logger = LogManager.getLogger(NewHash.class);

	RabbitMQConn mq = new RabbitMQConn();

	MongoConn mongo = new MongoConn();
	
	public NewHash() {
		super();
	}

	/**
	 * 处理Hash
	 */
	public void consume() throws IOException {
		//监听消息
		Channel channel = mq.getChannel();
		channel.queueDeclare(mqMongoNewQueue, true, false, false, null);
		channel.queueBind(mqMongoNewQueue, mqMongoExchange, mqMongoNewRouting);
		channel.basicConsume(mqMongoNewQueue, false, CONSUME_TAG, new DefaultConsumer(channel) {

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
				
				//2.2hash
				String shortHash = infoHash.substring(0, 16);
				logger.info("新资源: " + shortHash);

				//2.3处理文件列表
				int fileCount = 0;
				JSONArray filelist_5 = null;
				
				JSONArray filelist = data.getJSONArray("filelist");
				if (filelist != null) {
					fileCount = data.getIntValue("file_count");
					filelist_5 = filelist.size() > 5 ? new JSONArray(filelist.subList(0, 5)) : filelist;
				} else {
					fileCount = 0;
					filelist_5 = new JSONArray();
				}

				//2.4准备files数据
				Document file = null;
				
				if (filelist != null) {
					file = new Document();
					file.put("short_hash", shortHash);
					file.put("info_hash", infoHash);
					file.put("file_count", fileCount);
					file.put("file_list", filelist);
				}
				
				//2.5准备hash表数据
				Document hash = new Document();
				
				Date now = Calendar.getInstance().getTime();
				hash.put("short_hash", shortHash);
				hash.put("info_hash", infoHash);
				hash.put("category", data.getString("category"));
				hash.put("data_hash", data.getString("data_hash"));
				hash.put("name", data.getString("name"));
				hash.put("file_count", fileCount);
				hash.put("filelist_5", filelist_5);
				hash.put("extension", data.getString("extension") == null ? null : data.getString("extension").trim());
				hash.put("source_ip", data.getString("source_ip"));
				hash.put("length", data.getLong("length"));
				hash.put("create_time", now);
				hash.put("last_seen", now);
				hash.put("requests", 1);
				
				boolean spam = SpamAnalyzer.isSpam(data.getString("name"), filelist_5);
				if(spam)
					data.put("spam", spam);


				// 3保存数据
				try {
					mongo.save(SpiderConfig.COLL_HASH, hash);
					if (file != null)
						mongo.save(SpiderConfig.COLL_FILE, file);
				} catch (Exception e) {
					logger.error("保存失败：" + e.getMessage(), e);
					testMongoConn(mongo, channel, deliveryTag);
					return;
				}

				//4确认消息
				confirmMsg(channel, deliveryTag);
			}
			
		});
	}

	//test
	public static void main(String[] args) throws IOException {
		NewHash main = new NewHash();
		main.consume();
	}
}
