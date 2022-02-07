# yarn-application
How yarn application works - this is a sample yarn app which is created by me. 

In this example I create a app which pushes Kafka events, I submitted this app to Yarn, and it ran it.

##### What yarn is doing?
Actually it does, what you ask it to do. For example, you can move a Jar to HDFS and ask Yarn to run it using 
"java -jar app.jar". 

##### Asking for 2-3 containers to do the real work
This sample also asks for 3 nodes (1GB, 1 CPU) from resource manager
