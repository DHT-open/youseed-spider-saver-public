package org.rex.db.configuration;

import java.util.List;
import java.util.Properties;

import org.rex.DB;
import org.rex.RMap;
import org.rex.db.datasource.DataSourceFactory;
import org.rex.db.datasource.SimpleDataSourceFactory;
import org.rex.db.exception.DBException;

public class R {

	public static void main(String[] args) throws DBException {
		Properties props = new Properties();
		props.put("driverClassName", "com.mysql.jdbc.Driver");
		props.put("url", "jdbc:mysql://localhost:3306/zsky?serverTimezone=GMT%2B8");
		props.put("username", "root");
		props.put("password", "activezz1983");
		
		DataSourceFactory factory = new SimpleDataSourceFactory(props);
		Configuration conf = new Configuration();
		conf.setDefaultDataSource(factory.getDataSource());
		
		Configuration.setInstance(conf);
		
		List<RMap> ml = DB.getMapList("select * from complaint");
		System.out.println(ml);
	}
}
