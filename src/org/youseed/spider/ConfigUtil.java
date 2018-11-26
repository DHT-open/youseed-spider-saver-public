package org.youseed.spider;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.yaml.snakeyaml.Yaml;

import com.alibaba.fastjson.JSONObject;

/**
 * 读取配置文件
 */
public class ConfigUtil {
	
	/**
	 * 配置文件路径
	 */
	static final String DEFAULT_CONF = "config.yml";
	
	static String conf = null;
	
	private static Logger logger = LogManager.getLogger(ConfigUtil.class);
	
	//-------------------------------获取properties格式配置
	/**
	 * 设置配置路径
	 */
	public static void setConfPath(String path) {
		conf = path;
		logger.info("指定配置文件：" + path);
	}
	
	/**
	 * 获取扁平配置，便于取值
	 */
	public static JSONObject getProperties() {
		JSONObject prop = new JSONObject();
		JSONObject config = getConfig();
		iterMaps(prop, null, config);
		return prop;
	}
	
	private static void iterMaps(JSONObject prop, String key, JSONObject val) {
		for (Map.Entry<String, Object> entry : val.entrySet()) {
			
			String k = entry.getKey();
			Object v = entry.getValue();
			
			String flatKey = key == null ? k : key + "." + k;
			if(v instanceof Map) {
				iterMaps(prop, flatKey, new JSONObject((Map)v));
			}else {
				prop.put(flatKey, v);
			}
		}
	}

	
	//-------------------------------获取JSON格式配置
	/**
	 * 获取FastJSON类型配置
	 */
	public static JSONObject getConfig() {
		Yaml yaml = new Yaml();
		
		JSONObject config = null;
		if(conf == null) {
			logger.info("加载默认配置：" + DEFAULT_CONF);
			config = yaml.loadAs(Thread.currentThread().getContextClassLoader().getResourceAsStream(DEFAULT_CONF),
					JSONObject.class);
		}else {
			try {
				config = yaml.loadAs(new FileReader(conf), JSONObject.class);
				logger.info("加载配置：" + conf);
			} catch (FileNotFoundException e) {
				
				logger.error("加载配置文件出错：" + e.getMessage());
				logger.info("加载默认配置：" + DEFAULT_CONF);
				config = yaml.loadAs(Thread.currentThread().getContextClassLoader().getResourceAsStream(DEFAULT_CONF),
						JSONObject.class);
			}
		}
		
		return config;
	}
	
	
	//-------------------------------输出banner
	/**
	 * 输出文本内容，默认读取配置中的banner
	 */
	public static void printBanner() {
		printBanner(getConfig().getString("banner"));
	}
	
	/**
	 * 输出文本中的内容
	 */
	public static void printBanner(String path) {
		InputStream is = ConfigUtil.class.getClassLoader().getResourceAsStream(path);
		byte[] txt;
		try {
			txt = readStream(is);
			System.out.println(new String(txt));
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			try {
				is.close();
			} catch (IOException e) {
			}
		}
	}
	
	/** 
	 * 读取流 
	 */  
	private static byte[] readStream(InputStream inStream) throws Exception {  
	    ByteArrayOutputStream outSteam = new ByteArrayOutputStream();  
	    byte[] buffer = new byte[1024];  
	    int len = -1;  
	    while ((len = inStream.read(buffer)) != -1) {  
	        outSteam.write(buffer, 0, len);  
	    }  
	    outSteam.close();  
	    inStream.close();  
	    return outSteam.toByteArray();  
	} 
}
