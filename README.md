<img align="right"  src="apama.jpg">

# Apama ElasticSearch Connectivity Plug-in 

This Apama Connectivity Plug-in can be used to write Apama Events directly to ElasticSearch. This provides allows storage of Apama’s prescreened and enriched data to ElasticSearch’s query capability. This plugin can be quickly and easily installed and configured. Apama 10.0+ versions are supported.

For more information you can Ask a Question in the [TECHcommunity Forums](http://tech.forums.softwareag.com/techjforum/forums/list.page?product=webmethods-io-b2b).

You can find additional information in the [Software AG TECHcommunity](http://techcommunity.softwareag.com/home/-/product/name/webmethods-io-b2b).
______________________

These tools are provided as-is and without warranty or support. They do not constitute part of the Software AG product suite. Users are free to use, fork and modify them, subject to the license agreement. While Software AG welcomes contributions, we cannot guarantee to include every contribution in the master project.


## Prerequisits

- Apama Installation. Get the Apama Community Edition [here](https://www.apamacommunity.com/downloads/)
- Java SDK >= 1.8
- Apache Maven

## Build

Fetch  connectivity-plugins-api.jar from your Software AG Apama install directory (e.g. C:\Softwareag\Apama\lib) and install it as a Maven artifacts into local Maven repo:

	mvn install:install-file -Dfile=connectivity-plugins-api.jar -DgroupId=com.softwareag -DartifactId=connectivity-plugins-api -Dversion=10.5 -Dpackaging=jar
	
	mvn assembly:single
	
this should create the elasticsearchtransport.jar in the targets folder of this project.

## Run

Open the Apama Command Prompt and execute:

	correlator --config CorrelatorConfig.yaml --config elasticsearchplugin.properties

## Configuration 

The CorrelatorConfig.yaml file contains the configuration for different components: 

### CorrelatorConfig.yaml


All correlator specific parameters go here. E.g. which monitors should be injected:

	correlator:
     initialization:
        list:
            - ${APAMA_HOME}/monitors/ConnectivityPluginsControl.mon
            - ${APAMA_HOME}/monitors/ConnectivityPlugins.mon
            - ${PARENT_DIR}/elastic.mon
        encoding: UTF8

Change log levels via:

	correlatorLogging:
	  .root:
	    level: INFO
	  apama.connectivity: DEBUG
	  connectivity.ElasticSearchTransport.elastic: DEBUG
  
Which plug-ins should be loaded and where to find them:

	connectivityPlugins:
	  ElasticSearchTransport:
	    directory: ${PARENT_DIR}/target/
	    classpath: elasticsearchtransport.jar
	    class: com.softwareag.elasticsearchtransport.ElasticSearchPlugin

Chain definition. To send events to the plug in use the chain name (in this case elastic) as channel. 
	 
	startChains:
	  elastic:
	    - apama.eventMap:
	        allowMissing: true
	              
	    - ElasticSearchTransport:
	        port: ${elasticsearchplugin.port}
	        host: ${elasticsearchplugin.host}
	        user: ${elasticsearchplugin.user}
	        password: ${elasticsearchplugin.password}
	        index: ${elasticsearchplugin.index}
	        id: ${elasticsearchplugin.id}
			  
### elasticsearchplugin.properties

This config file contains mainly the Elasticsearch connection parameters and id and index settings.

	elasticsearchplugin.port=9200
	elasticsearchplugin.host=192.168.186.20
	elasticsearchplugin.user=elastic
	elasticsearchplugin.password=changeme
	elasticsearchplugin.id=apama,_,metadata.sag.type
	elasticsearchplugin.index=payload.id
	
## elastic.mon

This is an example Monitor to test the Elasticsearch connection and the proper function of the plug in. It will insert a new event every 5s into ES.


## Mapping Examples

index and id parameters can be evaluate out of event payload and metadata to distribute events to different indexes within Elasticsearch. 

### index

Indicates the index name to which the Event should be written. Supported is a list of comma separated values. Values can be a simple string or a path within the payload or metadata of the event.

The elastic.mon file contains an event 

	event Write{
			string id;
			string index;
			string name;
			string street;
			integer number;
					
		}

If we use this index mapping:

	elasticsearchplugin.index=apama,_,metadata.sag.type


		
In this case the event would be written to index: apama_elastic.write
This is the result of a two hardcoded strings (apama,_) and an evaluation of metadata.sag.type (wich results in elastic.write) 

	elasticsearchplugin.index=apama,_,payload.name
	
Lets say the field value of "name" of the event is "test". The event would be written to apama_test.

### Id

The mapping applies in the same way to the id. This can be blank if you want Elasticsearch to auto generate the id. 

	elasticsearchplugin.index=payload.id
	
This way the id will be fetched from the event itself. This is useful when those ids are already unique.


	
	

	




