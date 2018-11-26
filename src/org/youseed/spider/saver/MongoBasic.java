package org.youseed.spider.saver;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.youseed.spider.ConfigUtil;
import org.youseed.spider.MongoConn;
import org.youseed.spider.SpiderConfig;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.rabbitmq.client.Channel;

/**
 * 测试Mongo连接
 */
public class MongoBasic extends MQBasic{
	
	private static Logger logger = LogManager.getLogger(MongoBasic.class);
	
	protected String mqMongoExchange = "store";
	
	protected String mqMongoNewQueue = "store.new";
	protected String mqMongoNewRouting = "*.new";
	
	protected String mqMongoUpdateQueue = "store.update";
	protected String mqMongoUpdateRouting = "*.update";
	
	protected String mqMongoStatQueue = "store.stat";
	protected String mqMongoStatRouting = "*.stat";
	
	protected String collHash = "seed_hash";
	protected String collFilelist = "seed_filelist";
	protected String collStat = "seed_stat";
	
	/**
	 * 构造函数
	 */
	public MongoBasic() {
		ConfigUtil.printBanner();
		
		JSONObject config = ConfigUtil.getProperties();
		mqMongoExchange = config.containsKey("binding.mongo.exchage") ? config.getString("binding.mongo.exchage") : mqMongoExchange;
		mqMongoNewQueue = config.containsKey("binding.mongo.new.queue") ? config.getString("binding.mongo.new.queue") : mqMongoNewQueue;
		mqMongoNewRouting = config.containsKey("binding.mongo.new.routing") ? config.getString("binding.mongo.new.routing") : mqMongoNewRouting;
		mqMongoUpdateQueue = config.containsKey("binding.mongo.update.queue") ? config.getString("binding.mongo.update.queue") : mqMongoUpdateQueue;
		mqMongoUpdateRouting = config.containsKey("binding.mongo.update.routing") ? config.getString("binding.mongo.update.routing") : mqMongoUpdateRouting;
		mqMongoStatQueue = config.containsKey("binding.mongo.stat.queue") ? config.getString("binding.mongo.stat.queue") : mqMongoStatQueue;
		mqMongoStatRouting = config.containsKey("binding.mongo.stat.routing") ? config.getString("binding.mongo.stat.routing") : mqMongoStatRouting;
		
		collHash = config.containsKey("store.mongo.hash") ? config.getString("store.mongo.hash") : collHash;
		collFilelist = config.containsKey("store.mongo.filelist") ? config.getString("store.mongo.filelist") : collFilelist;
		collStat = config.containsKey("store.mongo.stat") ? config.getString("store.mongo.stat") : collStat;
		
		
		logger.info("---------RabbitMQ/Mongodb绑定配置------------");
		logger.info("交换器|binding.mongo.exchage: " + mqMongoExchange);
		logger.info("新资源队列|binding.mongo.new.queue: " + mqMongoNewQueue);
		logger.info("新资源路由|binding.mongo.new.routing: " + mqMongoNewRouting);
		logger.info("更新资源队列|binding.mongo.update.queue: " + mqMongoUpdateQueue);
		logger.info("更新资源路由|binding.mongo.update.routing: " + mqMongoUpdateRouting);
		logger.info("爬虫统计队列|binding.mongo.stat.queue: " + mqMongoStatQueue);
		logger.info("爬虫统计路由|binding.mongo.stat.routing: " + mqMongoStatRouting);
		logger.info("资源明细|store.mongo.index: " + collHash);
		logger.info("文件列表|store.mongo.type: " + collFilelist);
		logger.info("爬虫统计信息|store.mongo.stat: " + collStat);
		logger.info("---------------------------------------------");
	}
	
	/**
	 * 如果存在，则设置值
	 */
	private void setIfExists(JSONObject prop, String param, String key) {
		if(prop.containsKey(key)) {
			param = prop.getString(key);
		}
	}
	
	/**
	 * 批量更新hash，返回成功条目
	 */
	public int bulkUpdate(MongoConn mongo, JSONArray shortHashs){
		Date now = Calendar.getInstance().getTime();
		MongoCollection<Document> coll = mongo.getCollection(SpiderConfig.COLL_HASH);
		
		List<WriteModel<Document>> requests = new ArrayList<WriteModel<Document>>();
		for (int i = 0; i < shortHashs.size(); i++) {
			String shortHash = shortHashs.getString(i);
			UpdateOneModel<Document> uom = new UpdateOneModel<Document>(
					new Document("short_hash", shortHash), 
					new Document().append("$set", new Document("last_seen", now)).append("$inc", new Document("requests", 1)),
					new UpdateOptions().upsert(false));
			requests.add(uom);
		}
		BulkWriteResult bulkWriteResult = coll.bulkWrite(requests);
		return bulkWriteResult.getModifiedCount();
	}
	
	/**
	 * 测试Mongo连接是否正常，并进行相应的消息处理
	 */
	public void testMongoConn(MongoConn mongo, Channel channel, long deliveryTag) {
		
		boolean ok = testMongoConn(mongo, SpiderConfig.COLL_HASH);
		if(ok) {
			logger.info("mongodb连接正常，提交确认至消息队列");
			confirmMsg(channel, deliveryTag);
		} else {
			logger.info("mongodb连接失败，退回当前数据至队列");
			rejectMsg(channel, deliveryTag);
			
			try {
				logger.info("暂停60秒...");
				Thread.sleep(60000);
				return;
			} catch (InterruptedException e1) {
			}
		}
	}
	
	/**
	 * 测试Mongo连接
	 */
	public boolean testMongoConn(MongoConn mongo, String collName) {
		try {
			mongo.getCollection(collName).find().first();
			return true;
		} catch (Exception e) {
			return false;
		}
	}
}
