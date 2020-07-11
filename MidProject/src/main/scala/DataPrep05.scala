import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, regexp_replace, lit, rand, round, row_number, sum, avg, year, count}

object DataPrep05 {
 def main(args : Array[String]): Unit ={
   //Spark Session creation
   val spark = SparkSession.builder().appName("Mid project : Date preparation step 04").getOrCreate()
   //***************************************importing dataset from hive tables***************************************

   val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
   if ( ! fs.exists(new Path("taxi_trip_details_taxi_trip_id_removed_ts"))){
     spark.read.table("chicago_taxis.taxi_trip_details_taxi_trip_id_removed_ts").orderBy(rand(100000)).write.parquet("taxi_trip_details_taxi_trip_id_removed_ts")
   }
   val windowSpec = Window.partitionBy(lit(1)).orderBy(lit(1))
   val tripId = row_number().over(windowSpec)

   val taxitsdf = spark.read.format("parquet").load("taxi_trip_details_taxi_trip_id_removed_ts").withColumn("tripid", tripId).
     select(regexp_replace(col("company"),"^\\s*$","Z").as("company"), col("trip_start_date"), col("tripid"),col("trip_miles"), (round(col("trip_seconds")/60,2)).as("trip_duration"),col("trip_fare")).
     filter(col("company").isNotNull.and(col("company") =!= "Z"))

   // ***********************************************transformations**********************************************

   val compDateSumm = taxitsdf.groupBy(col("company"), col("trip_start_date")).
     agg(count("tripid").as("total_trips"), sum("trip_fare").as("total_fare"),sum("trip_miles").as("total_distance"),
       sum("trip_duration").as("total_duration"), avg("trip_fare").as("avg_fare"), avg("trip_miles").as("avg_distance"), avg("trip_duration").as("avg_duration"))

   val commYearlySumm = compDateSumm.groupBy(col("company"), year(col("trip_start_date")).as("year")).
     agg(sum("total_trips").as("daily_trip_count"), round(sum("total_fare"),2).as("daily_total_fare"),
       round(sum("total_distance"),2).as("daily_total_distance"), round(sum("total_duration"),2).as("daily_total_duration"), round(avg("avg_fare"),2).as("daily_average_amount"),
       round(avg("avg_distance"),2).as("daily_average_distance"), round(avg("avg_duration"),2).as("daily_average_duration")).orderBy("company","year")

   // *****************************************loading output to hive tables************************************

   commYearlySumm.write.mode("overwrite").format("csv").saveAsTable("edureka_735821.company_yearly_summary")
 }
}
