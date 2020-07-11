import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.streaming.kafka.KafkaUtils
import WebLogUtil.{logFields, logFormatValidate}

object WebLogAnalyzeStream_1s {

  def main(args : Array[String]): Unit ={

    val spark : SparkSession = SparkSession.builder().getOrCreate()
    val ssc : StreamingContext = new StreamingContext(spark.sparkContext, Seconds(1))
    spark.sparkContext.setLogLevel("ERROR")

    //----- creating dstream from kafka
    val dstr = KafkaUtils.createStream(ssc, "ip-20-0-21-196.ec2.internal:2181",
                            "edu_group_01", Map("weblog_735821" -> 1))


    //----- Cassandra connection
    val client : CassandraConnector = new CassandraConnector("cassandradb.edu.cloudlab.com", 9042, "edureka_735821")
    val session = client.getSession()

    println("-------------------------------------- Cassandra client created --------------------------------------")

    println("------------------------------------- Start of stream processing -------------------------------------")

    val log = dstr.map(record => record._2.toString)
      .filter( line => logFormatValidate(line))
      .map( log => logFields(log))
      .map( x => WebLogRecord(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8)))

    var batch_size : Int = 0
    log.foreachRDD( rdd => batch_size + rdd.count())

    //--------------------- detection of DOS attack ---------------------

    log.map( x => (x.ip,1) ).reduceByKey( (x:Int, y:Int) => x + y)
        .filter( x => x._2 >= 100 ).foreachRDD( rdd => {
      if (rdd.isEmpty()) {
        println("Empty microbatch")

        session.executeAsync("INSERT INTO FRAUD_LOGS (INSERT_DTTM, BATCH_SIZE) VALUES (TOTIMESTAMP(now()),"
          + batch_size +");")
      } else {
        rdd.collect.foreach( x => {
          println("!-------------------------------------!\n            Possible DOS attach        \n" +
          x._2 + " requests from " + x._1 + " in 1 second\n" +
          "!-------------------------------------!")

          session.execute("INSERT INTO FRAUD_LOGS (INSERT_DTTM, BATCH_SIZE, FRAUD_IP, ATTEMPTS ) VALUES (TOTIMESTAMP(now()), "
            + batch_size + ",'" + x._1 + "'," + x._2 + ");")
      }
        )
      }
      println(" - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -")
    })

    try{
      ssc.start()
      ssc.awaitTermination()
    } finally {
      ssc.stop(true, true)
      client.close()
      println("------------------------------------ Cassandra client closed ------------------------------------")
      println("------------------------------------ End of stream processing ------------------------------------")
      System.exit(1)
    }
  }
}
