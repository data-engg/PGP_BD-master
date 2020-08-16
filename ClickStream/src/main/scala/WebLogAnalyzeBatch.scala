/*
This is spark application to perform batch analytics on master data stored in hdfs
It can be invoked from the below  command :
spark2-submit --class WebLogAnalyzeBatch --jars ~/mysql-connector-java-5.1.49.jar \
--conf spark.dbc.user=... \
--conf spark.jdbc.password=... \
target/scala-2.11/click-stream-analysis_2.11-0.1.jar .
 */

import org.apache.spark.sql.SparkSession
import WebLogUtil.logFields

object WebLogAnalyzeBatch {
  def main(args: Array[String]): Unit = {

    val spark : SparkSession = SparkSession.builder().getOrCreate()

    import spark.implicits._
      val logdf = spark.sparkContext.textFile(args(0))
        .map(line => logFields(line))
        .map(x => WebLogRecord(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8))  )
        .toDF()

    logdf.createOrReplaceTempView("logdf")

    //---------- (i) Find the total number of unique visitors visited the website (IP indicates a unique user)

    spark.sql("select now() as ts, count(distinct ip) as visitor_cnt from logdf")
      .write.mode("append").format("jdbc").option("driver","com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://dbserver.edu.cloudlab.com/labuser_database")
      .option("dbtable","visitor_uniq_cnt_735821")
      .save()

    //---------- (ii) Find the total number of successful (200) v/s failure (non 200) response codes

    spark.sql(" select q1.key as dt, q1.http200 as http200, q2.other as other from (select  CURRENT_DATE() as key, count(*) as http200 from logdf where response = 200) q1 inner join (select  CURRENT_DATE() as key, count(*) as other from logdf where response <> 200) q2 on q1.key = q2.key")
      .write.format("jdbc").mode("append").option("driver","com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://dbserver.edu.cloudlab.com/labuser_database")
      .option("dbtable","sucss_fail_cnt_dly_735821")
      .save()

    //---------- (iii) Create a traffic distribution report, which contains day-wise unique visitors
    spark.sql("select dt, count(dt) as visitor_cnt from (select to_date(cast(UNIX_TIMESTAMP(ts, 'dd/MMM/yyyy') as TIMESTAMP)) as dt from logdf) q1 group by dt order by dt")
      .write.format("jdbc").mode("append").option("driver","com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://dbserver.edu.cloudlab.com/labuser_database")
      .option("dbtable","visitor_uniq_cnt_dly_735821")
      .save()

    //---------- (iv) Create a report which includes peak hour traffic distribution - analyze data to figure out usually which hours have maximum traffic on the website
    spark.sql("select hours, count(distinct ip) as visitor_cnt from (select hour(cast(UNIX_TIMESTAMP(ts, 'dd/MMM/yyyy:hh:mm:ss') as TIMESTAMP)) as hours, ip from logdf) q1 group by hours order by hours")
      .write.format("jdbc").mode("append").option("driver","com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://dbserver.edu.cloudlab.com/labuser_database")
      .option("dbtable","hourly_traffic_dist")
      .save()
  }
}
