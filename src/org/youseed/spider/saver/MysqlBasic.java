package org.youseed.spider.saver;

import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rex.DB;
import org.rex.db.exception.DBException;
import org.youseed.spider.ConfigUtil;

import com.alibaba.fastjson.JSONObject;
import com.rabbitmq.client.Channel;

/**
 * Mysql for zsky
 */
public class MysqlBasic extends MQBasic{
	
	private static Logger logger = LogManager.getLogger(MysqlBasic.class);
	
	protected String mqMysqlExchange = "store";
	
	protected String mqMysqlNewQueue = "store.new";
	protected String mqMysqlNewRouting = "*.new";
	
	protected String mqMysqlUpdateQueue = "store.update";
	protected String mqMysqlUpdateRouting = "*.update";
	
	protected String mqMysqlStatQueue = "store.stat";
	protected String mqMysqlStatRouting = "*.stat";
	
	protected String tableHash = "search_hash";
	protected String tableFilelist = "search_filelist";
	protected String tableStat = "search_statusreport";
	
	/**
	 * 构造函数
	 */
	public MysqlBasic() {
		ConfigUtil.printBanner();
		
		JSONObject config = ConfigUtil.getProperties();
		mqMysqlExchange = config.containsKey("binding.mysql.exchage") ? config.getString("binding.mysql.exchage") : mqMysqlExchange;
		mqMysqlNewQueue = config.containsKey("binding.mysql.new.queue") ? config.getString("binding.mysql.new.queue") : mqMysqlNewQueue;
		mqMysqlNewRouting = config.containsKey("binding.mysql.new.routing") ? config.getString("binding.mysql.new.routing") : mqMysqlNewRouting;
		mqMysqlUpdateQueue = config.containsKey("binding.mysql.update.queue") ? config.getString("binding.mysql.update.queue") : mqMysqlUpdateQueue;
		mqMysqlUpdateRouting = config.containsKey("binding.mysql.update.routing") ? config.getString("binding.mysql.update.routing") : mqMysqlUpdateRouting;
		mqMysqlStatQueue = config.containsKey("binding.mysql.stat.queue") ? config.getString("binding.mysql.stat.queue") : mqMysqlStatQueue;
		mqMysqlStatRouting = config.containsKey("binding.mysql.stat.routing") ? config.getString("binding.mysql.stat.routing") : mqMysqlStatRouting;
		
		tableHash = config.containsKey("store.mysql.hash") ? config.getString("store.mysql.hash") : tableHash;
		tableFilelist = config.containsKey("store.mysql.filelist") ? config.getString("store.mysql.filelist") : tableFilelist;
		tableStat = config.containsKey("store.mysql.stat") ? config.getString("store.mysql.stat") : tableStat;
		
		logger.info("---------RabbitMQ/Mysql-ZSKY绑定配置------------");
		logger.info("交换器|binding.mysql.exchage: " + mqMysqlExchange);
		logger.info("新资源队列|binding.mysql.new.queue: " + mqMysqlNewQueue);
		logger.info("新资源路由|binding.mysql.new.routing: " + mqMysqlNewRouting);
		logger.info("更新资源队列|binding.mysql.update.queue: " + mqMysqlUpdateQueue);
		logger.info("更新资源路由|binding.mysql.update.routing: " + mqMysqlUpdateRouting);
		logger.info("爬虫统计队列|binding.mysql.stat.queue: " + mqMysqlStatQueue);
		logger.info("爬虫统计路由|binding.mysql.stat.routing: " + mqMysqlStatRouting);
		logger.info("资源明细|store.mysql.index: " + tableHash);
		logger.info("文件列表|store.mysql.type: " + tableFilelist);
		logger.info("爬虫统计信息|store.mysql.stat: " + tableStat);
		logger.info("---------------------------------------------");
	}
	
	/**
	 * 测试Mysql连接是否正常，并进行相应的消息处理
	 */
	public void testMysqlConn(Channel channel, long deliveryTag) {
		
		boolean alive = false;
		try {
			alive = DB.getConnection().isValid(1000);//连接是否健在
		} catch (SQLException | DBException e) {
			logger.error("测试数据库连接失败："+e.getMessage(), e);
		}
		
		if(alive) {
			logger.info("Mysql连接正常，提交确认至消息队列");
			confirmMsg(channel, deliveryTag);
		} else {
			logger.info("Mysql连接失败，退回当前数据至队列");
			rejectMsg(channel, deliveryTag);
			try {
				logger.info("暂停60秒...");
				Thread.sleep(60000);
				return;
			} catch (InterruptedException e1) {
			}
		}
	} 
}
