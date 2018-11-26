package org.youseed.spider.saver;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.youseed.spider.ConfigUtil;
import org.youseed.spider.SpiderConfig;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.rabbitmq.client.Channel;

/**
 * 写入ES通用方法
 */
public class ESBasic extends MQBasic{
	
	private static Logger logger = LogManager.getLogger(ESBasic.class);
	
	protected String mqEsExchange = "es";
	
	protected String mqEsNewQueue = "es.new";
	protected String mqEsNewRouting = "*.new";
	
	protected String mqEsUpdateQueue = "es.update";
	protected String mqEsUpdateRouting = "*.update";
	
	protected String esIndex = "seed";
	protected String esType = "seed";
	
	/**
	 * 构造函数
	 */
	public ESBasic() {
		JSONObject config = ConfigUtil.getProperties();
		
		mqEsExchange = config.containsKey("binding.es.exchage") ? config.getString("binding.es.exchage") : mqEsExchange;
		mqEsNewQueue = config.containsKey("binding.es.new.queue") ? config.getString("binding.es.new.queue") : mqEsNewQueue;
		mqEsNewRouting = config.containsKey("binding.es.new.routing") ? config.getString("binding.es.new.routing") : mqEsNewRouting;
		mqEsUpdateQueue = config.containsKey("binding.es.update.queue") ? config.getString("binding.es.update.queue") : mqEsUpdateQueue;
		mqEsUpdateRouting = config.containsKey("binding.es.update.routing") ? config.getString("binding.es.update.routing") : mqEsUpdateRouting;
		esIndex = config.containsKey("store.es.index") ? config.getString("store.es.index") : esIndex;
		esType = config.containsKey("store.es.type") ? config.getString("store.es.type") : esType;
		
		logger.info("---------RabbitMQ/Elasticsearch绑定配置------------");
		logger.info("交换器|binding.es.exchage: " + mqEsExchange);
		logger.info("新资源队列|binding.es.new.queue: " + mqEsNewQueue);
		logger.info("新资源路由|binding.es.new.routing: " + mqEsNewRouting);
		logger.info("更新资源队列|binding.es.update.queue	: " + mqEsUpdateQueue);
		logger.info("更新资源路由|binding.es.update.routing: " + mqEsUpdateRouting);
		logger.info("索引|store.es.index	: " + esIndex);
		logger.info("索引类型|store.es.type: " + esType);
		logger.info("---------------------------------------------");
	}

	/**
	 * 批量更新hash，返回成功条目
	 */
	public int batchUpdateHash(TransportClient client, JSONArray data) {
		int now = (int)Calendar.getInstance().getTimeInMillis()/1000;
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("now", now);
		
		//1更新
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		for (int i = 0; i < data.size(); i++) {
			String shortHash = data.getString(i);
			UpdateRequest updateRequest = new UpdateRequest(SpiderConfig.ES_INDEX_SEED, SpiderConfig.ES_TYPE_SEED, shortHash);
			updateRequest.script(new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG,
					"ctx._source.requests+=1;ctx._source.last_seen=params.now", params));
			
			bulkRequest.add(updateRequest);
		}
		
		//2读取结果
		HashMap<String, String> errList = new HashMap<String, String>();
        BulkResponse resp = (BulkResponse)bulkRequest.execute().actionGet();
        if(resp.hasFailures()) {
            BulkItemResponse[] item = resp.getItems();
            for(int i = 0; i < item.length; i++) {
                if(item[i].isFailed()) {
                	errList.put(item[i].getId(), item[i].getFailure().getMessage());
                }
            }
           logger.error("失败条目: " + errList.size() + "|" + errList.values());
        }
        return data.size() - errList.size();
	}
	
	/**
	 * 测试ES连接是否正常，并进行相应的消息处理
	 */
	public void testEsConn(TransportClient client, Channel channel, long deliveryTag) {
		logger.info("测试 es连接....");
		try {
			NodesInfoResponse response = client.admin().cluster().nodesInfo(new NodesInfoRequest().timeout("30s")).actionGet();
			response.getNodesMap();
			
			logger.info("es连接正常，提交确认至消息队列");
			confirmMsg(channel, deliveryTag);
		} catch (Exception e) {
			logger.info("es连接失败，退回当前数据至队列", e);
			rejectMsg(channel, deliveryTag);
			
			try {
				logger.info("暂停60秒...");
				Thread.sleep(60000);
			} catch (InterruptedException e2) {
			}
		}
	}
}
