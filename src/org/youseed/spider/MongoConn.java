package org.youseed.spider;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

/**
 * 获取Mongo连接
 */
public class MongoConn {
	
	private static Logger logger = LogManager.getLogger(MongoConn.class);
	
	MongoClient mongoClient;
	MongoDatabase db;
	
	public MongoConn() {
		
		JSONObject mongo = ConfigUtil.getConfig().getJSONObject("mongo");
		
		String url = mongo.getString("url");
		int port = mongo.getIntValue("port");
		String dbName = mongo.getString("db");
		
		String user = mongo.getString("user");
		String admindb = mongo.getString("admindb");
		String psw = mongo.getString("psw");
		
		logger.info("---------Mongodb配置------------");
		logger.info("地址|mongo.url: " + url);
		logger.info("端口|mongo.port: " + port);
		logger.info("数据库|mongo.db: " + dbName);
		logger.info("用户名|mongo.user: " + user);
		logger.info("账户验证数据库|mongo.admindb: " + admindb);
		logger.info("---------------------------------------------");
		
		
		//设置连接超时
		MongoClientOptions options = MongoClientOptions.builder()
//				.threadsAllowedToBlockForConnectionMultiplier(20)
//				.connectTimeout(5000)
//				.maxWaitTime(5000)
//				.socketTimeout(5000)
				.build();
		
		
		if(mongo.get("user") == null) {
			ServerAddress add = new ServerAddress(url, port);
			mongoClient = new MongoClient(add, options);
			db = mongoClient.getDatabase(dbName);
		}else {
			ServerAddress add = new ServerAddress(url, port);
			List<ServerAddress> seeds = new ArrayList<ServerAddress>();
			seeds.add(add);
			
			MongoCredential cre =  MongoCredential.createCredential(user, admindb, psw.toCharArray());
			mongoClient = new MongoClient(seeds, cre, options);
			db = mongoClient.getDatabase(dbName);	
		}
		
	}
	
	/**
	 * 创建集合
	 */
	public void createColl(String collectionName) {
		db.createCollection(collectionName);
	}
	
	/**
	 * 创建唯一索引
	 */
	public void createIndexUnique(String collectionName, String index) {
		IndexOptions indexOptions = new IndexOptions();
		indexOptions.unique(true);
		getCollection(collectionName).createIndex(new Document().append(index, -1), indexOptions);
	}
	
	/**
	 * 创建唯一索引
	 */
	public void createIndexUnique(String collectionName, String[] index) {
		IndexOptions indexOptions = new IndexOptions();
		indexOptions.unique(true);
		
		Document idx = new Document();
		for (int i = 0; i < index.length; i++) {
			idx.append(index[i], 1);
		}
		
		getCollection(collectionName).createIndex(idx, indexOptions);
	}
	
	/**
	 * 创建索引
	 */
	public void createIndex(String collectionName, String index) {
		getCollection(collectionName).createIndex(new Document().append(index, -1));
	}
	
	/**
	 * 保存一个文档
	 */
	public void save(String collectionName, String json) {
		Document doc = Document.parse(json);
		save(collectionName, doc);
	}
	
	/**
	 * 保存一个文档
	 */
	public void save(String collectionName, Document doc) {
		MongoCollection<Document> collection = db.getCollection(collectionName, Document.class); 
		collection.insertOne(doc);
	}
	
	/**
	 * 保存多个文档
	 */
	public void save(String collectionName, List<Document> docs) {
		MongoCollection<Document> collection = db.getCollection(collectionName, Document.class); 
		InsertManyOptions options = new InsertManyOptions();//不开启验证/ 效率更快
	    options.ordered(false);
	    
		collection.insertMany(docs, options);
	}
	
	/**
	 * 获取一个文档
	 */
	public MongoCollection<Document> getCollection(String collectionName){
		return db.getCollection(collectionName); 
	}
	
	/**
	 * 获取文档中所有编号
	 */
	public Map<Object, Object> getIds(String collName) {
		Map<Object, Object> ids = new LinkedHashMap<Object, Object>();
		
		MongoCollection<Document> movieColl = getCollection(collName);
		Document fields = new Document().append("_id", 1);
		
		MongoCursor<Document> iter = movieColl.find().projection(fields).iterator();
		while(iter.hasNext()) {
			Document doc = iter.next();
			Object _id = doc.get("_id");
			ids.put(_id, null);
		}
		
		return ids;
	}
	
	/**
	 * 模糊查询
	 */
	public Map<Object, Object> getIdsFuzzy(String collName, String preg) {
		Map<Object, Object> ids = new LinkedHashMap<Object, Object>();
		
		MongoCollection<Document> movieColl = getCollection(collName);
		
		Pattern pattern = Pattern.compile(preg, Pattern.CASE_INSENSITIVE);
		BasicDBObject query = new BasicDBObject();
		query.put("_id", pattern);
		
		Document fields = new Document().append("_id", 1);
		
		MongoCursor<Document> iter = movieColl.find(query).projection(fields).iterator();
		while(iter.hasNext()) {
			Document doc = iter.next();
			Object _id = doc.get("_id");
			ids.put(_id, null);
		}
		
		return ids;
	}
	
	/**
	 * 删除一条记录
	 */
	public long delete(String collName, Object id) {
		MongoCollection<Document> coll = getCollection(collName);
		DeleteResult r = coll.deleteOne(new Document().append("_id", id));
		return r.getDeletedCount();
	}
	
	/**
	 * 删除一条记录
	 */
	public long delete(String collName, String key, Object val) {
		MongoCollection<Document> coll = getCollection(collName);
		DeleteResult r = coll.deleteOne(new Document().append(key, val));
		return r.getDeletedCount();
	}
	
	/**
	 * 删除符合条件的所有记录
	 */
	public long deleteMany(String collName, Document cond) {
		MongoCollection<Document> coll = getCollection(collName);
		DeleteResult r = coll.deleteMany(cond);
		return r.getDeletedCount();
	}
	
	
	/**
	 * 更新一条记录
	 */
	public long updateOne(String collName, Object id, Document cond) {
		MongoCollection<Document> coll = getCollection(collName);
		UpdateResult r = coll.updateOne(Filters.eq("_id", id), new Document("$set", cond));
		return r.getModifiedCount();
	}
	
	/**
	 * 更新所有记录
	 */
	public long updateAll(String collName, Document cond) {
		MongoCollection<Document> coll = getCollection(collName);
		UpdateResult r = coll.updateMany(Document.parse("{}"), cond);
		return r.getModifiedCount();
	}

}
