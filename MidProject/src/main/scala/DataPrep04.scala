import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lit, rand, row_number, col, round, concat, avg, split, sum, count}

object DataPrep04 {
  def main(args: Array[String]): Unit ={

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
      select(concat(col("pickup_community"),lit("-"), col("dropoff_community")).as("comm_pair"),col("tripid"),col("trip_miles"),
        (round(col("trip_seconds")/60,2)).as("trip_duration"),col("trip_fare"))

    // ***********************************************transformations**********************************************

    val originDestPairdf = taxitsdf.groupBy("comm_pair").agg(count("tripid").as("trip_count"), round(sum("trip_miles"),2).as("trip_total_miles"),
      round(avg("trip_miles"),2).as("avg_trip_miles"), round(avg("trip_duration"),2).as("avg_trip_duration"), round(avg("trip_fare"),2).as("avg_trip_fare")).na.drop("any").
      select(split(col("comm_pair"),"-").getItem(0).as("Community_origin"), split(col("comm_pair"),"-").getItem(1).as("Community_dest"),col("trip_count"), col("trip_total_miles"),
        col("avg_trip_miles"), col("avg_trip_duration"), col("avg_trip_fare")).orderBy(col("Community_origin").cast("int"),col("Community_dest").cast("int"))

    // *****************************************loading output to hive tables************************************
    originDestPairdf.coalesce(1).write.mode("overwrite").format("orc").saveAsTable("edureka_735821.ORIGIN_DEST_SUMMARY")
  }
}
