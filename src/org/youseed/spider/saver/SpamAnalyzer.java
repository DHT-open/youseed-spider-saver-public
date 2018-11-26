package org.youseed.spider.saver;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * 分析有无敏感词
 */
public class SpamAnalyzer {
	
	private static Logger logger = LogManager.getLogger(SpamAnalyzer.class);

	static final String SPAM = "spam.txt";
	
	static List<String> spams = readSpam();
	
	/**
	 * 判断是否敏感词，名称和文件列表（前5）都判断
	 */
	public static boolean isSpam(String name, JSONArray filelist5) {
		StringBuffer sb = new StringBuffer(name).append(" ");
		
		if(filelist5 != null) {
			for (int i = 0; i < filelist5.size(); i++) {
				JSONObject file = filelist5.getJSONObject(i);
				sb.append(file.getString("path")).append(" ");
			}
		}
		
		for(String spam : spams) {
			if(sb.toString().contains(spam)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * 读取关键字
	 */
	private static List<String> readSpam() {
		InputStreamReader ir = new InputStreamReader(SpamAnalyzer.class.getClassLoader().getResourceAsStream(SPAM));
		BufferedReader bf = new BufferedReader(ir);
		
		List<String> list = new ArrayList<String>();
		String str;
		try {
			// 按行读取字符串
			while ((str = bf.readLine()) != null) {
				list.add(str);
			}
			
			bf.close();
			ir.close();
		
		} catch (IOException e) {
		}

		//加载关键词
		logger.info("敏感关键词数量：" + list.size());
		return list;
	}
	
	//test
	public static void main(String[] args) {
		System.out.println(isSpam("porn", null));
	}
}
