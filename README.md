Apama ElasticSearch  Connectivity Plugin.

Can be used to write Apama Events directrly to ElasticSearch. It can only one eventtype be written at a time. For multiple Events configure multiple chains.

Build:

- Import as a maven project.
- Install needed Maven artifacts into local Maven repo (Fetch from SAG Installation).
- Build using Maven Target assembly:single

Run:
- Include in a Apama Project in the Lib Folder
- Configure Chains and properties 
- Configure ElasticSearch Host/Port/User/Password Index Type


