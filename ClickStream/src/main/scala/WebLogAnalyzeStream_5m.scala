import WebLogUtil.{logFields, logFormatValidate}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, StreamingContext}
import com.datastax.driver.core.exceptions.SyntaxError
object WebLogAnalyzeStream_5m {

  def main(args : Array[String]): Unit ={
    val spark : SparkSession = SparkSession.builder().getOrCreate()
    val ssc : StreamingContext = new StreamingContext(spark.sparkContext, Minutes(5))
    spark.sparkContext.setLogLevel("ERROR")

    //----- creating dstream from kafka
    val dstr = KafkaUtils.createStream(ssc, "ip-20-0-21-196.ec2.internal:2181",
      "edu_group_02", Map("weblog_735821" -> 1))

    //----- Cassandra connection
    val client : CassandraConnector = new CassandraConnector("cassandradb.edu.cloudlab.com",
      9042, "edureka_735821")
    val session = client.getSession()

    println("-------------------------------------- Cassandra client created --------------------------------------")
    println("------------------------------------- Start of stream processing -------------------------------------")

    val log = dstr.map(record => record._2.toString)
      .filter( line => logFormatValidate(line))
      .map( log => logFields(log))
      .map( x => WebLogRecord(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8)))

    var batch_size : Int = 0
    log.foreachRDD( rdd => batch_size + rdd.count())

    //--------------------- Issue prediction ---------------------


    log.map( x => (x.response,1)).reduceByKeyAndWindow((x:Int, y:Int) => x + y, Minutes(5))
      .filter( x => x._1 != "200" && x._2 > 10)
      .foreachRDD( rdd => {
        if (rdd.isEmpty()){
          println("Empty microbatch")
          session.executeAsync("INSERT INTO ERROR_LOGS (INSERT_DTTM, BATCH_SIZE) VALUES " +
            "(TOTIMESTAMP(now()),"+ batch_size +");")
        } else {
          rdd.collect.foreach( x => {
            println("Check system \n HTTP : " + x._1 + " returned " + x._2 + " times in 300 seconds")

            session.executeAsync("INSERT INTO ERROR_LOGS (INSERT_DTTM, BATCH_SIZE, HTTP_CODE, REQ_COUNT) " +
              "VALUES (TOTIMESTAMP(now())," + batch_size + "," + x._1 +"," + x._2 + ");")
          }
          )
        }
        println(" - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -")
      }
      )


    //--------------------- Category popularity ---------------------
    log.map( x => (x.request.split("/")(1),1))
      .filter( x => ! x._1.contains("HTTP"))
      .reduceByKeyAndWindow((x:Int, y:Int) => x + y, Minutes(5) )
      .foreachRDD( rdd => {
        var cat_str = rdd.collectAsMap().toString().replace("(","{").replace(")","}")
          .replaceAll("->",":")
          .substring(3).replaceAll( "([a-z\\-]+)", "\'$1\'" )

        val query = "INSERT INTO POPULARITY_LOGS (INSERT_DTTM, POPULARITY_MAP) VALUES" +
          "(TOTIMESTAMP(now())," + cat_str + ");"

        try{
          session.execute(query)
        } catch {
          case se : SyntaxError => {
            println("Error is query syntax")
            println("Executed query : " + query)
          }
          case e : Exception => {
            e.printStackTrace()
          }
        }


        println(" = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =")

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
