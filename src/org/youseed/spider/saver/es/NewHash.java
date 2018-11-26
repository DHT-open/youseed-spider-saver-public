package org.youseed.spider.saver.es;

import java.io.IOException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.youseed.spider.ConfigUtil;
import org.youseed.spider.ESConn;
import org.youseed.spider.RabbitMQConn;
import org.youseed.spider.SpiderConfig;
import org.youseed.spider.saver.ESBasic;
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
 * 写入新资源到ES
 */
public class NewHash extends ESBasic {
	
	/**
	 * 消费者标签
	 */
	static final String CONSUME_TAG_PREFIX = "es-new-consumer";
	
	private static Logger logger = LogManager.getLogger(NewHash.class);
	
	RabbitMQConn mq = new RabbitMQConn();
	
	ESConn es  = new ESConn();

	/**
	 * 构造函数
	 */
	public NewHash() {
		super();
		logger.info("starting new hash consumer...");
	}
	
	/**
	 * 处理Hash
	 */
	public void consume() throws IOException {
		
		//只入库指定的分类
		String catsConf = ConfigUtil.getConfig().getString("includeCategories");
		final List<String> cats = StringUtil.isNullOrEmpty(catsConf) ? null : Arrays.asList(catsConf.split(","));

		//开始连接MQ
		TransportClient client = es.getClient();
		Channel channel = mq.getChannel();
		
		channel.queueDeclare(mqEsNewQueue, true, false, false, null);
		channel.queueBind(mqEsNewQueue, mqEsExchange, mqEsNewRouting);
		channel.basicConsume(mqEsNewQueue, false, CONSUME_TAG_PREFIX, new DefaultConsumer(channel) {

			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
					byte[] body) {
				
				long deliveryTag = envelope.getDeliveryTag();

				//1解析数据
				JSONObject data = null;
				try {
					data = JSON.parseObject(new String(body, "UTF-8"));
				} catch (Exception e) {
					logger.error("JSON解析失败，提交成功并跳过消息：" + e.getMessage(), e);
					confirmMsg(channel, deliveryTag);
					return;
				}
				
				//2分析数据
				//2.1格式校验
				String infoHash = data.getString("info_hash");
				String cat = data.getString("category");
				String name = data.getString("name");
				if(StringUtil.isNullOrEmpty(cat) || StringUtil.isNullOrEmpty(infoHash) || StringUtil.isNullOrEmpty(name)) {
					logger.info("数据格式不正确，跳过消息");
					confirmMsg(channel, deliveryTag);
					return;
				}
				
				//2.2检查分类
				if(cats != null) {
					//2.2.1类型不在设定范围，跳过消息
					if(!cats.contains(cat)) {
						confirmMsg(channel, deliveryTag);
						logger.info("skip: " + cat);
						return;
					}
					
					
					//2.2.2如果只设定了一个类型，将文件扩展名视为分类
					if(cats.size() == 1) {
						String ext = data.getString("extension").trim();
						ext = ext.substring(1);
						data.put("category", ext);
						data.remove("extension");
					}
					
					logger.info("store: " + data.get("category"));
				}
				
				//2.2hash
				String shortHash = infoHash.substring(0, 16);
				logger.info("新资源: " + shortHash);

				//2.3如果没有文件，写空数据
				if(!data.containsKey("filelist_5")) {
					data.put("file_count", 0);
					data.put("filelist_5", new JSONArray());
				}
				
				//2.4敏感词（校验name和fileslist_5）
				boolean spam = SpamAnalyzer.isSpam(name, data.getJSONArray("filelist_5"));
				if(spam) {
					data.put("spam", true);
				}
				
				//2.5日期
				long now = Calendar.getInstance().getTimeInMillis()/1000;
				data.put("create_time", now);
				data.put("last_seen", now);
				
				//2.6移除多余字段
				data.remove("data_hash");
				data.remove("extension");
				data.remove("source_ip");
				
				//3写入搜索引擎
				try {
					IndexResponse resp = client.prepareIndex(SpiderConfig.ES_INDEX_SEED, SpiderConfig.ES_TYPE_SEED)
						.setId(shortHash).setSource(data).execute().actionGet();
					
				}catch(Exception e) {
					logger.error("写入Hash出错: " + e.getMessage(), e);
					testEsConn(client, channel, deliveryTag);
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
