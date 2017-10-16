Apama ElasticSearch  Connectivity Plugin.

Can be used to write Apama Events directly to ElasticSearch. It can only one eventtype be written at a time. For multiple Events configure multiple chains.

Build:

- Import as a maven project.
- Install needed Maven artifacts into local Maven repo (Fetch from SAG Installation).
- Build using Maven Target assembly:single

Run:
- Include the jar in a Apama Project in the Lib Folder and add as a custom connectivity plugin.

-Configure ElasticSearch Host/Port/User/Password Index Type

Config:

- ElasticSearch Connection
  Host/Port/User/Password

- Type / Index
  Can be hardcoded or mapped from Event payload or Metadata

  Examples:

	index: "payload.source_id,_,payload.type"
	type:  "payload.type"  
	
	index: Will combile the value of the field source_id and type of the event's first level to the ElasticSearch Index. All comma seperated values that start with matadata or payload will evaluated other string values will just be concatenated.
	type: Will on use type for ElasticSearch type. 

