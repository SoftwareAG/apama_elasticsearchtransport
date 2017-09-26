package com.softwareag.elasticsearchtransport;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.softwareag.connectivity.AbstractTransport;
import com.softwareag.connectivity.MapHelper;
import com.softwareag.connectivity.Message;
import com.softwareag.connectivity.PluginConstructorParameters.TransportConstructorParameters;

/**
 * Hello world!
 *
 */
public class ElasticSearchPlugin extends AbstractTransport
{
	final private int PORT;
	final private String HOST;
	final private String USER;
	final private  String PASSWORD;
	final private  String INDEX;
	final private  String TYPE;
	final CredentialsProvider credentialsProvider;
	private RestClient restClient;
	private RestHighLevelClient esClient;
    public ElasticSearchPlugin(Logger logger, TransportConstructorParameters params)
			throws IllegalArgumentException, Exception {
		super(logger, params);
		PORT = (int)MapHelper.getInteger(config, "port");
		HOST = (String)MapHelper.getString(config, "host");
		USER = (String)MapHelper.getString(config, "user");
		PASSWORD = (String)MapHelper.getString(config, "password");
		INDEX = (String)MapHelper.getString(config, "index");
		TYPE = (String)MapHelper.getString(config, "type");
		credentialsProvider= new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY,
		        new UsernamePasswordCredentials(USER,PASSWORD));
	}


    
    public IndexResponse insertString(String index, String type, String strToInsert) throws IOException {
    	logger.debug("Index: " + index + " Type: " + type + " Doc: " + strToInsert); 
		IndexRequest request = new IndexRequest(index);
		request.type(type);
		request.source(strToInsert,XContentType.JSON);
		IndexResponse indexResponse = esClient.index(request);
		return indexResponse;
	}

    @Override
    public void hostReady() throws Exception {
    	logger.info("Start Elastic Search Host Ready");

    	RestClientBuilder builder = RestClient.builder(new HttpHost(HOST, PORT))
		        .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
		            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
		                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
		            }
		        });
		
		restClient = builder.build();
		esClient = new RestHighLevelClient(restClient);
		boolean clusterUp = false;
		MainResponse response = null;
		
		while (!clusterUp) {
			try {
				response = esClient.info();
			} catch (IOException e ) {
				logger.error("Could not connect ot Elastic Search. Retrying. " + e.getMessage());
				Thread.sleep(5000);
				continue;
			}
			String clusterName = response.getClusterName().value();
			if (response != null && !clusterName.equalsIgnoreCase("")) {
				clusterUp = true;
				logger.info("Elastic Search Client Successfully Started on Cluster: " + clusterName);
				break;
			}else {
				logger.error("Could not get cluster name. Retrying. ");
				Thread.sleep(5000);
			}
		}

    }
    
    @Override
	public void shutdown() throws Exception {
    	restClient.close();
		logger.info("Elastic Search Client Stopped");
		
	}
	
    
	@Override
	public void sendBatchTowardsTransport(List<Message> messages) {
		String index =INDEX;
		String type = TYPE;
		for (Message message : messages) {
			
			// Extracting Metadata
			Map<String, String> metaMap = message.getMetadata();
			logger.debug("Metadata: " + metaMap.toString());
			if (INDEX.startsWith("metadata")) {
				String indexKey = INDEX.substring(INDEX.indexOf(".") +1,INDEX.length());
				logger.debug("IndexKey: "+ indexKey);
				index= metaMap.get(indexKey);
				logger.debug("index: "+ index);
			}
			if (TYPE.startsWith("metadata")) {
				String typeKey = TYPE.substring(TYPE.indexOf(".") +1,TYPE.length());
				logger.debug("TypeKey: "+ typeKey);
				type= metaMap.get(typeKey);
				logger.debug("type: "+ type);
			}
			Map<String, Object> payloadMap;
			if (  !(message.getPayload() instanceof Map)) {
				logger.info("Payload is not a Map skipping.");
				continue;
			}
	
			
			// Extracting payload
			payloadMap = (Map<String, Object>) message.getPayload();
			ObjectMapper mapper = new ObjectMapper();
			String jsonString = "";
			try {
				jsonString = mapper.writeValueAsString(payloadMap);
			} catch (JsonProcessingException e1) {
				logger.error("Could map event to json Document.", e1.getMessage());
				continue;
			}
			
			// Indexing Document
			IndexResponse resp;
			try {
				resp = insertString(index, type, jsonString);
				logger.debug("Document insert response = " + resp.getResult().getLowercase()); 
			} catch (IOException e) {
				logger.error("Could not Index Document.", e.getMessage());
				continue;
			}
		}

		
	}
}
