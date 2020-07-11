import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{row_number, lit, col, count,  year, month,split, dense_rank,ceil, floor, round, concat, when, avg, sum, rand}
import org.apache.hadoop.fs.{FileSystem, Path}

object DataPrep01 {
  def main(args: Array[String]): Unit = {

    if (args.length < 1 ){
      println("Please enter the directory where results are expected")
      System.exit(1)
    }
    val result_dir:String = args(0)

    //session creation
    val spark = SparkSession.builder().appName("Mid project : Date preparation step 01").getOrCreate()

    //***************************************importing dataset from hive tables***************************************

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
     if ( ! fs.exists(new Path("taxi_trip_details_taxi_trip_id_removed_ts"))){
       spark.read.table("chicago_taxis.taxi_trip_details_taxi_trip_id_removed_ts").orderBy(rand(100000)).write.parquet("taxi_trip_details_taxi_trip_id_removed_ts")
     }
    if ( ! fs.exists(new Path("taxi_trip_details_weekend_encoded"))){
      spark.read.table("chicago_taxis.taxi_trip_details_weekend_encoded").orderBy(rand(100000)).write.parquet("taxi_trip_details_weekend_encoded")
    }

    val windowSpec = Window.partitionBy(lit(1)).orderBy(lit(1))
    val tripId = row_number().over(windowSpec)

    val taxitsdf = spark.read.parquet("taxi_trip_details_taxi_trip_id_removed_ts").withColumn("tripid", tripId)
    val taxiweekenddf = spark.read.parquet("taxi_trip_details_weekend_encoded").withColumn("tripid", tripId)

    taxitsdf.cache()

    // ***********************************************transformations**********************************************

    //Windows and Ranks
    val communityWinSpec = Window.orderBy(col("total_trips").desc)
    val fareWinn = Window.orderBy(col("total_trips").desc)
    val distWinn = Window.orderBy(col("total_trips").desc)
    val tripCntWin = Window.orderBy(col("avg_trip_count").desc)
    val tripAmtWin = Window.orderBy(col("avg_daily_total").desc)

    val rankComm = dense_rank().over(communityWinSpec)
    val fareRank = dense_rank().over(fareWinn)
    val tripRank = dense_rank().over(distWinn)

    // 1. Trips per year

    val yearTripId = taxitsdf.select(col("trip_start_date"), col("tripid")).withColumn("year", split(col("trip_start_date"), "-").getItem(0)).drop(col("trip_start_date"))
    val tripPerYear = yearTripId.groupBy(col("year")).agg(count(col("tripid")))

    // 2. Trips per year per month

    val yearMonTripid = taxitsdf.select( year(col("trip_start_date")).as("year"), month(col("trip_start_date")).as("month"), col("tripid") )
    val tripPerYearPerMonth = yearMonTripid.groupBy(col("year")).pivot("month").agg(count(col("tripid")))

    // 3. Percentage of records that contain drop-off community value

    val total_records = taxitsdf.count()
    val community_trips = taxitsdf.select(col("dropoff_community")).na.drop("all").groupBy( col("dropoff_community") ).agg( count(col("dropoff_community")).as("total_trips"))
    val percRecordsDropOffVal = community_trips.withColumn("rankComm", rankComm).filter( col("rankComm") < 11).
      withColumn("Percentage", (col("total_trips")*100/total_records) ).drop(col("rankComm"))

    // 4. Total number of trips for each drop-off community across each year

    val tripPerCommPerYear = taxitsdf.select(col("dropoff_community"), year(col("trip_start_date")).as("year"), col("tripid")).na.drop("any").
      groupBy("dropoff_community","year").agg(count("tripid").as("total_trips"))
    val tripPerYearForComm = tripPerCommPerYear.withColumn("rank", rankComm).filter( col("rank") < 11).drop( col("rank"))

    // 5. Total community trips on weekends and weekdays

    val weekenddf = taxiweekenddf.select(col("dropoff_community"), col("weekend")).na.drop("any").filter( col("weekend") === 1 ).drop(col("weekend"))
    val weekdaydf = taxiweekenddf.select(col("dropoff_community"), col("weekend")).na.drop("any").filter( col("weekend") === 0 ).drop(col("weekend"))
    val weekendtrips = weekenddf.groupBy(col("dropoff_community")).agg(count("dropoff_community").as("total_trips")).withColumn("rank",rankComm).
      filter(col("rank") < 11).drop(col("rank"))
    val weekdaytrips = weekdaydf.groupBy(col("dropoff_community")).agg(count("dropoff_community").as("total_trips")).withColumn("rank",rankComm).
      filter(col("rank") < 11).drop(col("rank"))

    weekdaytrips.createOrReplaceTempView("weekday")
    weekendtrips.createOrReplaceTempView("weekend")

    val CommTripWkdWed = spark.sql("select wd.dropoff_community, wd.total_trips +  we.total_trips as total_trips, round(wd.total_trips/we.total_trips,2) as ratio from weekday wd inner join weekend we on wd.dropoff_community=we.dropoff_community")

    // 6. Total number of trips based on trip duration

      val trip_time_dist = taxitsdf.select(round(col("trip_seconds")/3600,2).as("duration_hours"), col("tripid")).na.drop("any").
      withColumn("floor",concat(floor(col("duration_hours")))).withColumn("ceil", ceil(col("duration_hours"))).
      withColumn("range",
        when(col("ceil")===0, "0-1").
          when( col("floor") =!= col("ceil"), concat(floor(col("duration_hours")),lit("-"), ceil(col("duration_hours")))).
          when ( col("floor") === col("ceil"), concat(floor(col("duration_hours")-1),lit("-"), ceil(col("duration_hours"))))).
      drop("duration_hours", "ceil","floor")

    val tripTimeDist = trip_time_dist.groupBy("range").agg(count("tripid")).orderBy(split(col("range"),"-").getItem(0).cast("int"))

    // 7. Top 10 buckets of the number of trips distribution based on the distance covered

    val trip_per_dist = taxitsdf.select(round(col("trip_miles")).as("RoundOff_dist")).na.drop("any").filter(col("RoundOff_dist") > 0.0).groupBy("RoundOff_dist").agg(count("*").as("total_trips"))
    val tripdistCov = trip_per_dist.withColumn("rank", tripRank).filter( col("rank") < 11 ).drop("rank")

    // 8. Top 10 buckets of the number of trips distribution based on the trip fare

    val trip_per_fare = taxitsdf.select(round(col("trip_fare")).as("RoundOff_fare")).filter(col("RoundOff_fare")>0).
      groupBy(col("RoundOff_fare")).agg(count("*").as("total_trips"))
    val tripfare = trip_per_fare.withColumn("rank", fareRank).filter( col("rank") < 11 ).drop("rank")

    // 9. the average trip fare per day based weekday and weekend wise

    val weekenddf2 = taxiweekenddf.select( col("trip_start_date"), col("trip_total_amt"), col("weekend"), col("tripid")).filter( col("weekend") === 1).drop("weekend")
    val weekdaydf2 = taxiweekenddf.select( col("trip_start_date"), col("trip_total_amt"), col("weekend"), col("tripid")).filter( col("weekend") === 0).drop("weekend")

    val avgTripFarePerDayWeekday = weekdaydf2.groupBy(col("trip_start_date")).agg(round(avg("trip_total_amt"),2).as("avg_trip_fare"))
    val avgTripFarePerDayWeekend = weekenddf2.groupBy(col("trip_start_date")).agg(round(avg("trip_total_amt"),2).as("avg_trip_fare"))

    /* 10.  a. Find the top 10 taxis based on average trips per day.
	          b. Find the top 10 taxis based on average fare per day.*/
    val TripPerDay = taxiweekenddf.select(col("tripid"), col("taxi_id_int"), col("start_dayofweek"), col("trip_total_amt")).groupBy(col("taxi_id_int"),col("start_dayofweek")).
      agg(count("tripid").as("trip_count")).groupBy(col("taxi_id_int")).agg(round(avg("trip_count"),2).as("avg_trip_count"))
    val avgTripPerDay = TripPerDay.withColumn("rank", dense_rank.over(tripCntWin)).filter(col("rank")<11).drop(col("rank"))

    val TripAmtPerDay = taxiweekenddf.select(col("tripid"), col("taxi_id_int"), col("start_dayofweek"), col("trip_total_amt")).groupBy(col("taxi_id_int"),col("start_dayofweek")).
      agg(sum("trip_total_amt").as("trip_count")).groupBy(col("taxi_id_int")).agg(round(avg("trip_count"),2).as("avg_daily_total"))

    val avgTripAmtPerDay = TripAmtPerDay.withColumn("rank", dense_rank.over(tripAmtWin)).filter(col("rank")<11).drop(col("rank"))

    // ***************************************************outputs***************************************************

    tripPerYear.coalesce(1).write.csv(result_dir + "/DataPrep01/01")
    tripPerYearPerMonth.coalesce(1).write.csv(result_dir +"/DataPrep01/02")
    percRecordsDropOffVal.coalesce(1).write.csv(result_dir + "/DataPrep01/03")
    tripPerYearForComm.coalesce(1).write.csv(result_dir + "/DataPrep01/04")
    weekendtrips.coalesce(1).write.csv(result_dir + "/DataPrep01/05/weekend")
    weekdaytrips.coalesce(1).write.csv(result_dir + "/DataPrep01/05/weekday")
    CommTripWkdWed.coalesce(1).write.csv(result_dir + "/DataPrep01/05/comp")
    tripTimeDist.coalesce(1).write.csv(result_dir + "/DataPrep01/06")
    tripdistCov.coalesce(1).write.csv(result_dir + "/DataPrep01/07")
    tripfare.coalesce(1).write.csv(result_dir + "/DataPrep01/08")
    avgTripFarePerDayWeekday.coalesce(1).write.csv(result_dir + "/DataPrep01/09/weekday")
    avgTripFarePerDayWeekend.coalesce(1).write.csv(result_dir + "/DataPrep01/09/weekend")
    avgTripPerDay.coalesce(1).write.csv(result_dir + "/DataPrep01/10/tripCount")
    avgTripAmtPerDay.coalesce(1).write.csv(result_dir + "/DataPrep01/10/tripAmount")

    // **********************************************delete temp files**********************************************

    taxitsdf.unpersist()
  }
}
