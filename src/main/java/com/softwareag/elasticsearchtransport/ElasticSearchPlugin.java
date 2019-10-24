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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.softwareag.connectivity.AbstractTransport;
import com.softwareag.connectivity.Message;
import com.softwareag.connectivity.PluginConstructorParameters.TransportConstructorParameters;
import com.softwareag.connectivity.util.MapExtractor;

/**
 * Apama ElasticSearch Connectivity Plugin
 *
 */
public class ElasticSearchPlugin extends AbstractTransport
{
	final private int PORT;
	final private String HOST;
	final private String USER;
	final private  String PASSWORD;
	final private  String INDEX_PATTER;
	final private  String ID_PATTERN;
	final private String[] mappingsIndex;
	final private String[] mappingsId;
	final CredentialsProvider credentialsProvider;
	private RestHighLevelClient esClient;
	
    public ElasticSearchPlugin(Logger logger, TransportConstructorParameters params)
			throws IllegalArgumentException, Exception {
		super(logger, params);
		MapExtractor config = new MapExtractor(params.getConfig(), "configuration");
		PORT = Integer.valueOf(config.getStringDisallowEmpty("port"));
		HOST = config.getStringDisallowEmpty("host");
		USER = config.getStringDisallowEmpty("user");
		PASSWORD = config.getStringDisallowEmpty("password");
		INDEX_PATTER = config.getStringDisallowEmpty( "index");
		ID_PATTERN = config.getStringAllowEmpty( "id");
		mappingsIndex = INDEX_PATTER.split(",");
		mappingsId = ID_PATTERN.split(",");
		credentialsProvider= new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY,
		        new UsernamePasswordCredentials(USER,PASSWORD));
	}


    
    public  void insertString(String index, String id, String strToInsert) throws IOException {
    	
    	logger.debug("Index: " + index + " DocumentID: " + id + " Doc: " + strToInsert); 
		IndexRequest ir = new IndexRequest(index.toLowerCase());
		ir.source(strToInsert,XContentType.JSON);
		if (id!= null && !id.equals("")) {
			ir.id(id);
		}
		
		ActionListener<IndexResponse> listener = new ActionListener<IndexResponse>() {
		    @Override
		    public void onResponse(IndexResponse indexResponse) {
		        logger.debug("Successfull written:" + indexResponse.toString());
		    }

		    @Override
		    public void onFailure(Exception e) {
		    	logger.error("Error writing IndexRequest:" + e.getMessage());
		    }
		};
		
		esClient.indexAsync(ir, RequestOptions.DEFAULT, listener );
		
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

		esClient = new RestHighLevelClient(builder);
		boolean clusterUp = false;
		ClusterHealthResponse response = null;
		ClusterHealthRequest chr = new ClusterHealthRequest();
		
		
		while (!clusterUp) {
			try {
				response = esClient.cluster().health(chr, RequestOptions.DEFAULT);
			} catch (IOException e ) {
				logger.error("Could not connect ot Elastic Search. Retrying. " + e.getMessage());
				Thread.sleep(5000);
				continue;
			}
			String clusterName = response.getClusterName();
			if (response != null && !clusterName.equalsIgnoreCase("")) {
				clusterUp = true;
				logger.info("Elastic Search Client Successfully Started on Cluster: " + clusterName + " Cluster Health: " + response.getStatus() );
				break;
			}else {
				logger.error("Could not get cluster name. Retrying. ");
				Thread.sleep(5000);
			}
		}

    }
    
    @Override
	public void shutdown() throws Exception {
    	try {
			esClient.close();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error("Error stopping Elastic Search Client: " +e.getMessage());
		}
		logger.info("Elastic Search Client Stopped");
		
	}
	
    
	public void sendBatchTowardsTransport(List<Message> messages) {
		
		for (Message message : messages) {
			
			
			Map<String, Object> payloadMap;
			Map<String, Object> metaMap;
			String index = "";
			String id = "";

			if (message.getPayload() instanceof Map && message.getMetadataMap() instanceof Map ) {
				// Extracting Metadata				
				metaMap = message.getMetadataMap();
				// Extracting payload
				payloadMap = (Map<String, Object>) message.getPayload();
				logger.debug("Payload: " + payloadMap.toString());
				logger.debug("Metadata: " + metaMap.toString());
				index = evalMapping(metaMap,payloadMap,mappingsIndex);
				id = evalMapping(metaMap,payloadMap,mappingsId);
				
				logger.debug("index: "+ index);
				logger.debug("id: "+ id);
				
				
				// Convert Apama Event To JSON
				ObjectMapper mapper = new ObjectMapper();
				String jsonString = "";
				try {
					jsonString = mapper.writeValueAsString(payloadMap);
				} catch (JsonProcessingException e1) {
					logger.error("Could map event to json Document.", e1.getMessage());
					continue;
				}
				
				// Indexing Document
				try {
					insertString(index, id, jsonString);
				} catch (IOException e) {
					logger.error("Could not Index Document.", e.getMessage());
					continue;
				}
				
			} else {
				logger.info("Payload Or Metadata is not a Map skipping.");
			}
		}

		
		
	}
	
	private String evalMapping(Map<String, Object> metaMap,Map<String, Object> payloadMap, String[] mappingArray) {
		
		String mapping = "";
		logger.debug("Payload: " + payloadMap.toString());
		logger.debug("Metadata: " + metaMap.toString());
		for (int i = 0; i < mappingArray.length; i++) {
			// Get Map field
			String mappingfield = mappingArray[i].substring(mappingArray[i].indexOf(".") +1,mappingArray[i].length());
			if (mappingArray[i].startsWith("metadata")) {
				mapping += metaMap.get(mappingfield);
			} else if(mappingArray[i].startsWith("payload")){
				mapping += payloadMap.get(mappingfield);
			}else if (!mappingArray[i].equals("")) {
				mapping  += mappingArray[i];
			}else {
				logger.error("Could not get Mapping. Settin mapping to empty string");
				continue;
			}
		}
		
		logger.debug("mapping: "+ mapping);
		return mapping;
	}

}
