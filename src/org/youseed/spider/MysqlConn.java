package org.youseed.spider;

import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rex.db.configuration.Configuration;
import org.rex.db.datasource.DataSourceFactory;
import org.rex.db.datasource.SimpleDataSourceFactory;
import org.rex.db.exception.DBException;

import com.alibaba.fastjson.JSONObject;

public class MysqlConn {

	private static Logger logger = LogManager.getLogger(MysqlConn.class);
	
	/**
	 * 数据库已初始化？
	 */
	public static boolean inited = false;
	
	/**
	 * 初始化数据库
	 */
	public static synchronized void initDB() {
		if(inited) return;

		JSONObject mysql = ConfigUtil.getConfig().getJSONObject("mysql");
		
		Properties props = new Properties();
		props.put("driverClassName", "com.mysql.jdbc.Driver");
		props.put("url", mysql.getString("url"));
		props.put("username", mysql.getString("user"));
		props.put("password", mysql.getString("psw"));
		
		try {
			DataSourceFactory factory = new SimpleDataSourceFactory(props);
			Configuration conf = new Configuration();
			conf.setDefaultDataSource(factory.getDataSource());
			Configuration.setInstance(conf);
			inited = true;
			
			logger.error("已初始化Mysql连接");
		} catch (DBException e) {
			logger.error("初始化Mysql连接失败：" + e.getMessage(), e);
		}
		
	}
}
