package org.youseed;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.youseed.spider.ConfigUtil;
import org.youseed.spider.saver.mongo.SaveStat;

import com.alibaba.fastjson.JSONObject;

/**
 * 执行入口
 */
public class Main {
	
	private static Logger logger = LogManager.getLogger(Main.class);
	
	public static void main(String[] args) throws IOException {
		
		String order = null;
		
		//1.分析参数，支持 “--config=/opt/config.yml”、“m1”这两种
		if ((args != null) && (args.length > 0)) {
			for (int i = 0; i < args.length; i++) {
				//设置配置路径
				if(args[i].startsWith("--config=")) {
					String path = args[i].substring(9);
					
					if(!"".equals(path.trim()))
						ConfigUtil.setConfPath(path);
				}else {
					order = args[i];
				}
			}
		} 
		
		if(order == null){
			order = getOperId();
		}
		
		//2.输出banner
		ConfigUtil.printBanner();
		
		//3.执行选择的操作
		
		//--Mongodb写入+更新操作
		if ("m".equals(order)) {
			new org.youseed.spider.saver.mongo.NewHash().consume();
			new org.youseed.spider.saver.mongo.UpdateHash().consume();
			new SaveStat().consume();
		} 

		//--写入新资源到Mongodb
		else if ("m1".equals(order)) {
			new org.youseed.spider.saver.mongo.NewHash().consume();
		} 
		
		//--更新资源到Mongodb
		else if ("m2".equals(order)) {
			new org.youseed.spider.saver.mongo.UpdateHash().consume();
		} 
		
		//--写入爬虫日志到Mongodb
		else if ("m3".equals(order)) {
			new SaveStat().consume();
		} 
		
		//--ES的写入和更新操作（根据esUpdateTime设置自动选择实时或定时更新）
		else if ("es".equals(order)) {
			
			//1.启动写入新资源的消费者
			new org.youseed.spider.saver.es.NewHash().consume();
			
			//2.根据定时配置，启动持续或者定时更新消费者
			JSONObject config = ConfigUtil.getProperties();
			String t = config.getString("esUpdateTime");
			
			//2.1没设置时间，常驻监听模式
			if(t == null || "".equals(t.trim())) {
				logger.info("常驻内存随时更新");
				new org.youseed.spider.saver.es.UpdateHash().consume();
				return;
			}
			
			Date time = null;
			SimpleDateFormat  sdf = new SimpleDateFormat("HH:mm:ss");
			try {
				Calendar calNow = Calendar.getInstance();
				
				Calendar cal = Calendar.getInstance();
				cal.setTime(sdf.parse(t));
				cal.set(Calendar.YEAR, calNow.get(Calendar.YEAR));
				cal.set(Calendar.MONTH, calNow.get(Calendar.MONTH));
				cal.set(Calendar.DATE, calNow.get(Calendar.DATE));
				
				if (cal.getTime().before(calNow.getTime())) {  
					cal.add(Calendar.DAY_OF_MONTH, 1);  
		        } 
				
				time = cal.getTime();
				
			} catch (ParseException e) {
				logger.error("格式化定时更新ES出错" + e.getMessage(), e);
			}
			
			//2.2.格式化时间错误，常驻监听模式
			if(time == null) {
				logger.info("常驻内存随时更新");
				new org.youseed.spider.saver.es.UpdateHash().consume();
				return;
			}
			
			//2.3.时间设置无误，启动定时任务
			logger.info("已设置为定时更新资源，每日更新时间：" + sdf.format(time) + 
					"，首次执行时间：" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time));
			
			Timer timer = new Timer();
	        timer.schedule(new TimerTask(){
				@Override
				public void run() {
					try {
						logger.info("开始执行更新任务");
						new org.youseed.spider.saver.es.UpdateHashOnTime().consume();
					} catch (IOException e) {
						logger.error("更新任务执行出错" + e.getMessage(), e);
					}
					logger.info("更新任务执行完毕");
				}
	        }, time, 24 * 60 * 60 * 1000);
			
		}
		
		//--写入新资源到ES
		else if ("es1".equals(order)) {
			new org.youseed.spider.saver.es.NewHash().consume();
		} 
		
		//--更新资源到ES（常驻并实时更新）
		else if ("es2".equals(order)) {
			new org.youseed.spider.saver.es.UpdateHash().consume();
		} 
		
		//--更新资源到ES（更新完毕当前批次后关闭）
		else if ("es3".equals(order)) {
			new org.youseed.spider.saver.es.UpdateHashOnTime().consume();
		}
		
		//--zsky写入+更新操作
		if ("zsky".equals(order)) {
			new org.youseed.spider.saver.zsky.NewHash().consume();
			new org.youseed.spider.saver.zsky.UpdateHash().consume();
			new org.youseed.spider.saver.zsky.SaveStat().consume();
		} 

		//--写入新资源到zsky
		else if ("zsky1".equals(order)) {
			new org.youseed.spider.saver.zsky.NewHash().consume();
		} 
		
		//--更新资源到zsky
		else if ("zsky2".equals(order)) {
			new org.youseed.spider.saver.zsky.UpdateHash().consume();
		} 
		
		//--写入爬虫日志到zsky
		else if ("zsky3".equals(order)) {
			new org.youseed.spider.saver.zsky.SaveStat().consume();
		} 
	}

	public static String getOperId() {
		Map<String, String> oper = new LinkedHashMap();
		oper.put("m", "写入/更新Mongodb");
		oper.put("m1", "\t|-------写入新资源到Mongo");
		oper.put("m2", "\t|-------更新Mongo");
		oper.put("m3", "\t|-------写入统计到Mongo");
		oper.put("es", "写入/更新ES（根据esUpdateTime设置，自动选择实时或定时更新）");
		oper.put("es1", "\t|-------写入新资源到ES");
		oper.put("es2", "\t|-------更新ES（常驻内存并实时更新）");
		oper.put("es3", "\t|-------更新ES（更新完毕当前批次后关闭）");
		oper.put("zsky", "写入/更新纸上烤鱼（zsky）");
		oper.put("zsky1", "\t|-------写入新资源到Mysql");
		oper.put("zsky2", "\t|-------更新Mysql");
		oper.put("zsky3", "\t|-------写入统计到Mysql");	

		Scanner sc = new Scanner(System.in);

		String order = null;
		for (;;) {
			if (oper.containsKey(order)) {
				sc.close();
				return order;
			}
			for (Map.Entry<String, String> entry : oper.entrySet()) {
				String id = (String) entry.getKey();
				String desc = (String) entry.getValue();

				System.out.println(id + ": " + desc);
			}
			System.out.println("");
			System.out.println("请选择一项操作（输入编号后回车）:");

			order = sc.next();
		}
	}
}
