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
	final private  String INDEX_PATTER;
	final private  String TYPE_PATTERN;
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
		INDEX_PATTER = (String)MapHelper.getString(config, "index");
		TYPE_PATTERN = (String)MapHelper.getString(config, "type");
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
		
		for (Message message : messages) {
			
			
			Map<String, Object> payloadMap;
			Map<String, String> metaMap;
			String index = "";
			String type = "";

			if (message.getPayload() instanceof Map && message.getMetadata() instanceof Map ) {
				// Extracting Metadata				
				metaMap = message.getMetadata();
				// Extracting payload
				payloadMap = (Map<String, Object>) message.getPayload();
				logger.debug("Payload: " + payloadMap.toString());
				logger.debug("Metadata: " + metaMap.toString());
				String[] mappingsIndex = INDEX_PATTER.split(",");
				String[] mappingsType = TYPE_PATTERN.split(",");
				for (int i = 0; i < mappingsIndex.length; i++) {
					String mappingfield = mappingsIndex[i].substring(mappingsIndex[i].indexOf(".") +1,mappingsIndex[i].length());
					if (mappingsIndex[i].startsWith("metadata")) {
						index += metaMap.get(mappingfield);
					} else if(mappingsIndex[i].startsWith("payload")){
						index += payloadMap.get(mappingfield);
					}else if (!mappingsIndex[i].equals("")) {
						index  += mappingsIndex[i];
					}else {
						logger.error("Could not get Index. Skipping. Please provide payload. or metadata. as prefix.  Or use a String");
						continue;
					}
				}
				for (int i = 0; i < mappingsType.length; i++) {
					String mappingfield = mappingsType[i].substring(mappingsType[i].indexOf(".") +1,mappingsType[i].length());
					if (mappingsType[i].startsWith("metadata")) {
						type += metaMap.get(mappingfield);
					} else if(mappingsType[i].startsWith("payload")){
						type += payloadMap.get(mappingfield);
						
					}else if (!mappingsType[i].equals("")) {
						type += mappingsType[i];
					}
					else {
						logger.error("Could not get Type. Skipping. Please provide payload. or metadata. as prefix. Or use a String");
						continue;
					}
				}
				logger.debug("index: "+ index);
				logger.debug("type: "+ type);
				
			} else {
				logger.info("Payload Or Metadata is not a Map skipping.");
				continue;
			}

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
