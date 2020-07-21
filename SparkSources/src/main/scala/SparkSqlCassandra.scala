/*
  This is simple code to write data from hdfs and write to cassandra.
  Command used to submit this program is :

  spark2-submit --class SparkSqlCassandra \
  --jars $SPARK_HOME/jars/spark-cassandra-connector_2.11-2.0.12.jar,$SPARK_HOME/jars/jsr166e-1.1.0.jar \
  --conf spark.cassandra.auth.username=... --conf spark.cassandra.auth.password=... \
   target/scala-2.11/spark_sources_2.11-0.1.jar station/ demo_keyspace station

   Output table cannot be create on fly. It must be present and there must be exact mapping between fields of dataframe and table

 */

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.datastax.driver.core.exceptions.AuthenticationException
import scala.collection.immutable.Map

object SparkSqlCassandra {

  def main(args : Array[String]): Unit ={

    if ( args.length != 3){
      println("Enter exactly 3 arguements")
      println("Usage : spark2-submit --class SparkSqlCassandra <path to jar> <hdfs path of source file> <cassandra keyspace> <table>")
      System.exit(1)
    }

    val conf = new SparkConf(true)

    /*Cassandra configuration. Authentication credentials to the supplied at runtime
        --conf spark.cassandra.auth.username=...
        --conf spark.cassandra.auth.password=...
     */
    conf.set("spark.cassandra.connection.host","cassandradb.edu.cloudlab.com")
    conf.set("park.cassandra.connection.port","9042")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    //reading csv file from hdfs
    val station = spark.read.format("csv").options(Map("inferSchema"->"true", "header"->"false")).load(args(0))


    //batch loading to cassandra
    try {
      station.write.format("org.apache.spark.sql.cassandra").options(Map("keyspace" -> args(1), "table" -> args(2))).save()
    } catch {
      case authexc : AuthenticationException => {
        println("Authentication credentials not provied at run time or incorrect credentails provided")
      } case exc : ClassNotFoundException => {
        println("Check runtime jars supplied")
      }
    }

  }
}
