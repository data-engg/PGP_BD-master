import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{lit, rand, row_number, col, count, sum, avg, round}

object DataPrep03 {

  def main(args: Array[String]): Unit ={

    //Spark Session creation
    val spark = SparkSession.builder().appName("Mid project : Date preparation step 03").getOrCreate()

    //***************************************importing dataset from hive tables***************************************
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    if ( ! fs.exists(new Path("taxi_trip_details_taxi_trip_id_removed_ts"))){
      spark.read.table("chicago_taxis.taxi_trip_details_taxi_trip_id_removed_ts").orderBy(rand()).write.parquet("taxi_trip_details_taxi_trip_id_removed_ts")
    }
    val windowSpec = Window.partitionBy(lit(1)).orderBy(lit(1))
    val tripId = row_number().over(windowSpec)

    val taxitsdf = spark.read.parquet("taxi_trip_details_taxi_trip_id_removed_ts").withColumn("tripid", tripId)
    taxitsdf.cache()

    /***********************************************transformations**********************************************
     *
     * 1. Pickup community daily summary*/

    val comPickUpDayAggDF = taxitsdf.select( col("pickup_community"), col("trip_start_date"), col("tripid"), col("trip_fare"), col("trip_miles"), (col("trip_seconds")/60).as("trip_duration")).
      groupBy(col("pickup_community"), col("trip_start_date")).
      agg(count("tripid").as("tripid"), sum("trip_fare").as("total_trip_fare"), sum("trip_miles").as("total_distance"), sum("trip_duration").as("trip_duration"),
        avg("trip_fare").as("avg_fare"), avg("trip_miles").as("avg_distance"), avg("trip_duration").as("avg_duration") )

    val comPickUpDailySumm = comPickUpDayAggDF.groupBy(col("pickup_community")).agg(sum("tripid").as("daily_trip_count"), round(sum("total_trip_fare"),2).as("daily_total_fare"),
      round(sum("total_distance"),2).as("daily_total_distance"), round(avg("avg_fare"),2).as("daily_avg_fare"), round(avg("avg_distance"),2).as("daily_avg_distance"),
      round(avg("avg_duration"),2).as("daily_avg_duration")).orderBy(col("daily_trip_count").desc)

    // 2. Dropoff community daily summary

    val comDropOffDayAggDF = taxitsdf.
      select( col("pickup_community"), col("trip_start_date"), col("tripid"), col("trip_fare"), col("trip_miles"), (col("trip_seconds")/60).as("trip_duration")).
      groupBy(col("pickup_community"), col("trip_start_date")).
      agg(count("tripid").as("tripid"), sum("trip_fare").as("total_trip_fare"), sum("trip_miles").as("total_distance"), sum("trip_duration").as("trip_duration"), avg("trip_fare").as("avg_fare"),
        avg("trip_miles").as("avg_distance"), avg("trip_duration").as("avg_duration") )

    val comDropOffDailySumm = comPickUpDayAggDF.groupBy(col("pickup_community")).agg(sum("tripid").as("daily_trip_count"), round(sum("total_trip_fare"),2).as("daily_total_fare"),
      round(sum("total_distance"),2).as("daily_total_distance"), round(avg("avg_fare"),2).as("daily_avg_fare"), round(avg("avg_distance"),2).as("daily_avg_distance"),
      round(avg("avg_duration"),2).as("daily_avg_duration")).orderBy(col("daily_trip_count").desc)


    // *****************************************loading output to hive tables************************************

    comPickUpDailySumm.write.mode("overwrite").format("csv").saveAsTable("edureka_735821.CommunityPickUpDailySummary")
    comDropOffDailySumm.write.mode("overwrite").format("csv").saveAsTable("edureka_735821.CommunityDropOffDailySummary")


  }
}
