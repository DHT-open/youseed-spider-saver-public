package org.youseed.spider;

public class SpiderConfig {
	
	//==================MQ配置
	public static final String MQ_VIRTUAL_HOSTS = "/";

	//----用于Mongodb存储
	//入库的数据
	public static final String MQ_STORE_EXCHANGE = "store";
	
	//新hash
	public static final String MQ_STORE_HASH_QUEUE = "store.new";
	public static final String MQ_STORE_HASH_ROUTING = "store.new";
	
	//待更新热度的hash
	public static final String MQ_STORE_UPDATE_QUEUE = "store.update";
	public static final String MQ_STORE_UPDATE_ROUTING = "store.update";
	
	//爬虫统计信息
	public static final String MQ_STORE_SPIDER_QUEUE = "store.stat";
	public static final String MQ_STORE_SPIDER_ROUTING = "store.stat";

	//----用于写搜索引擎
	//搜索引擎的数据
	public static final String MQ_ES_EXCHANGE = "es";
	
	//新hash
	public static final String MQ_ES_HASH_QUEUE = "es_movie.new";
	public static final String MQ_ES_HASH_ROUTING = "es_movie.new";
	
	//待更新热度的hash
	public static final String MQ_ES_UPDATE_QUEUE = "es_movie.update";
	public static final String MQ_ES_UPDATE_ROUTING = "es_movie.update";
	
	
	//==================Mongo配置
	public static final String COLL_HASH = "seed_hash";
	public static final String COLL_FILE = "seed_filelist";
	public static final String COLL_STATE = "seed_stat";
	
	
	//==================ES配置
	public static final String ES_INDEX_SEED="seed";
	public static final String ES_TYPE_SEED = "seed";
	
}
