This is a capstone project about click stream analytics. Sources from two webservers (weblog1.txt and weblog2.txt)are stored in a hdfs location. These files are pushed to spooling directory. From there they are fanned out to kafka topic and a hdfs sink (master copy). This entire thing is a simulation of streaming data.

Data ingestion : Data is ingested into the system by flume agents. They are :-
    agent1 : This agent validates clickstream data from weblog1.txt and forwards it to spooldir to avro sink. 
    agent2 : This agent validates clickstream data from weblog2.txt and forwards it to spooldir to avro sink. 
    agent3 : The avro sink in previous case fans out data received from agent1 & agent2 to kafka topic and hdfs sink (master copy).

The analysis of clickstream data follows a lambda architecture. The description of layers are : -

1. Speed layer : For data to be processed in real time, a spark streaming job pulls data from a kafka topic at 1 second interval and performs the following analysis :

    i. DDOS attack : Checks if there are more than 100 requets from an IP in a second
    ii. Server health : Checks if there are more than 10 requests with http respose other than 200 in 5 minutes
    iii. Popularity check : Calcuate the popularity of categories in last 5 minutes and store it to a Cassandra DB
    
2. Batch layer : For batch analysis on master data, a spark sql job performs the following analysis and stores result to RDBMS:

  i. Total number of unique visitors visiting the website on a day
  ii. Total number of success vs failure response code
  iii. Distribution of day wise unique visitor count
  iv. Hourly distribution of traffic
  
3. Serving layer : The results of the speed layer and batch layer is presented for analysis in :
  
  CassandraDB --> The output of streaming job is written to Cassandra because, 
                    i. For most times the output will be null
                    ii. Low latency writes
                    iii. Eventual consistency is acceptable
                    
  RDBMS --> The output of batch job is written to RDBMS because,
                    i. Simplicity of operation
                    ii. Output is highly structured and well formatted with very few or no nulls
                    iii. Write latency is acceptable 
