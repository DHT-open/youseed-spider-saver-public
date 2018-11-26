package org.youseed.spider;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.alibaba.fastjson.JSONObject;

/**
 * Elasticsearch
 */
public class ESConn {
	
	private static Logger logger = LogManager.getLogger(ESConn.class);
	
	private TransportClient client = null;
	
	public ESConn() {
		JSONObject config  = ConfigUtil.getConfig().getJSONObject("es");
		String url = config.getString("url");
		int port = config.getIntValue("port");
		
		logger.info("---------Elasticsearch配置------------");
		logger.info("地址|es.url: " + url);
		logger.info("端口|es.port: " + port);
		logger.info("---------------------------------------------");
		
		try {
			client = new PreBuiltTransportClient(Settings.EMPTY)
					.addTransportAddress(new TransportAddress(InetAddress.getByName(url), port));
		} catch (UnknownHostException e) {
			throw new RuntimeException(e);
		}
	}
	

	public TransportClient getClient() {
		return client;
	}
	
	public IndexRequestBuilder getindexBuilder(String indexName) {
		return client.prepareIndex(indexName, "main");
	}

	/**
	 */
	public void indexData(String indexName, String json) {
		IndexResponse response = client.prepareIndex(indexName, "main").setSource(json, XContentType.JSON).execute()
				.actionGet();
	}
	
	/**
	 */
	public void indexData(String indexName, String[] json) {
		IndexRequestBuilder builder = client.prepareIndex(indexName, "main");
		for (int i = 0; i < json.length; i++) {
			builder.setSource(json[i], XContentType.JSON);
		}
		builder.execute();
	}
}
