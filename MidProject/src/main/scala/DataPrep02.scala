import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{lit, rand, row_number, col, round, sum, mean, year, count, avg, month, weekofyear, udf}
import java.util.Calendar
import java.text.SimpleDateFormat

object DataPrep02 {
  def main(args: Array[String]): Unit = {

    //Spark Session creation
    val spark = SparkSession.builder().appName("Mid project : Date preparation step 02").getOrCreate()

    //***************************************importing datasets from hive tables***************************************
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    if ( ! fs.exists(new Path("taxi_trip_details_weekend_encoded"))){
      spark.read.table("chicago_taxis.taxi_trip_details_weekend_encoded").orderBy(rand()).write.parquet("taxi_trip_details_taxi_trip_id_removed_ts")
    }
    val windowSpec = Window.partitionBy(lit(1)).orderBy(lit(1))
    val tripId = row_number().over(windowSpec)

    val taxiweekend = spark.read.parquet("taxi_trip_details_weekend_encoded").withColumn("tripid", tripId)

    // ************************************************creating udfs*********************************************
    val weekStartDate = spark.udf.register("weekStartDate", (week : Short, year : Short) =>{
      var cal:Calendar = Calendar.getInstance()
      val dateform: SimpleDateFormat = new SimpleDateFormat("MM/dd/yyyy")
      cal.clear()
      cal.set(Calendar.WEEK_OF_YEAR, week)
      cal.set(Calendar.YEAR, year)
      cal.add(Calendar.DATE, 1)
      dateform.format(cal.getTime()).toString()
    })

      val weekEndDate = spark.udf.register("weekEndDate", (week : Short, year : Short) => {
      var cal:Calendar = Calendar.getInstance()
      val dateform: SimpleDateFormat = new SimpleDateFormat("MM/dd/yyyy")
      cal.clear()
      cal.set(Calendar.WEEK_OF_YEAR, week)
      cal.set(Calendar.YEAR, year)
      cal.add(Calendar.DATE, 7)
      dateform.format(cal.getTime()).toString()
    })

    val monthStartDate = spark.udf.register("monthStartDate" , (month : Short, year : Short) =>{
      var cal:Calendar = Calendar.getInstance();
      val dateform: SimpleDateFormat = new SimpleDateFormat("MM/dd/yyyy")
      cal.clear()
      cal.set(Calendar.MONTH, month-1)
      cal.set(Calendar.YEAR, year)
      dateform.format(cal.getTime()).toString()
    })

    val monthEndDate = spark.udf.register("monthEndDate", (month : Short, year :Short) => {
      var cal:Calendar = Calendar.getInstance();
      val dateform: SimpleDateFormat = new SimpleDateFormat("MM/dd/yyyy")
      cal.clear()
      cal.set(Calendar.MONTH, month-1)
      cal.set(Calendar.YEAR, year)
      cal.set(Calendar.DAY_OF_MONTH, cal.getActualMaximum(Calendar.DATE))
      dateform.format(cal.getTime()).toString()
    })


    // ***********************************************transformations********************************************

    //1. Daily Summary table
    val dailySummaryNormalAvgDf = taxiweekend.select(col("trip_start_date").as("date"), col("start_dayofweek").as("dayOfWeek"), month(col("trip_start_date")).as("month"),
      year(col("trip_start_date")).as("year"), col("weekend"), col("tripid"), col("trip_fare"), col("trip_miles"),
      (col("trip_seconds")/60).as("trip_duration")).
      groupBy(col("date"), col("dayOfWeek"), col("month"), col("year"), col("weekend")).
      agg(count("tripid").as("trip_count"), round(sum("trip_fare"),2).as("Total_Trip_fare"), round(sum("trip_miles"),2).as("Total_Trip_miles"),
        sum("trip_duration").as("Total_trip_duration"), mean("trip_fare").as("avg_trip_fare"), mean("trip_miles").as("avg_trip_miles"),
        mean("trip_duration").as("avg_trip_duration"))

    val daily_avg_trip_count = dailySummaryNormalAvgDf.select("trip_count").agg(avg("trip_count")).head.getDouble(0)

    val dailySummaryDf = dailySummaryNormalAvgDf.select(col("date"), col("dayOfWeek").as("Day_of_week"), col("month"), col("year"),
      col("weekend").as("Weekend/Weekday"), col("trip_count").as("Total_Trip_count"), col("Total_Trip_fare"), col("Total_Trip_miles"), col("Total_trip_duration"),
      (round(col("avg_trip_fare") * col("trip_count") / daily_avg_trip_count,4)).as("Avg_Trip_fare"), (round(col("avg_trip_miles") * col("trip_count") / daily_avg_trip_count,4)).as("Avg_Trip_miles"),
      (round(col("avg_trip_duration") * col("trip_count") / daily_avg_trip_count,4)).as("Avg_trip_duration")).orderBy(col("date").cast("date"))

    //2. Weekly Summary table
    val weeklySummaryNormalAvgDf =  spark.read.table("edureka_735821.dailysummary").withColumn("weekOfYear", weekofyear(col("date"))).drop("Weekend/Weekday","Day_of_week","month").
      orderBy(col("year"), col("weekOfYear")).groupBy(col("year"), col("weekOfYear")).agg(count("Total_Trip_count").as("trip_count"), round(sum("Total_Trip_fare"),2).as("Total_Trip_fare"),
      round(sum("Total_Trip_miles"),2).as("Total_Trip_miles"),  sum("Total_trip_duration").as("Total_trip_duration"), mean("Avg_Trip_fare").as("avg_trip_fare"),
      mean("Avg_Trip_miles").as("avg_trip_miles"),  mean("Avg_trip_duration").as("avg_trip_duration"))

    val weekly_avg_trip_count = weeklySummaryNormalAvgDf.select("trip_count").agg(avg("trip_count")).head.getDouble(0)

    val weeklySummaryDf = weeklySummaryNormalAvgDf.select(col("year"), col("weekOfYear"),
      col("trip_count").as("Total_Trip_count"), col("Total_Trip_fare"), col("Total_Trip_miles"), round(col("Total_trip_duration"),2).as("Total_trip_duration"),
      (round(col("avg_trip_fare") * col("trip_count") / weekly_avg_trip_count,4)).as("Avg_Trip_fare"), (round(col("avg_trip_miles") * col("trip_count") / weekly_avg_trip_count,4)).as("Avg_Trip_miles"),
      (round(col("avg_trip_duration") * col("trip_count") / weekly_avg_trip_count,4)).as("Avg_trip_duration")).withColumn("month_no",tripId)

    val weeklySummary = weeklySummaryDf.select(col("week_no") ,weekStartDate(col("weekOfYear"), col("year")).as("date_from"), weekEndDate(col("weekOfYear"), col("year")).as("date_to"),
      col("Total_Trip_count"), col("Total_Trip_fare"), col("Total_Trip_miles"), col("Total_trip_duration"), col("Avg_Trip_fare"), col("Avg_Trip_miles"), col("Avg_trip_duration"))


    //3. Monthly Summary table

    val monthlySummaryNormalAvgDf = spark.read.table("edureka_735821.dailysummary").orderBy(col("year"),col("month")).groupBy(col("year"),col("month")).
      agg(count("Total_Trip_count").as("trip_count"), round(sum("Total_Trip_fare"),2).as("Total_Trip_fare"),
        round(sum("Total_Trip_miles"),2).as("Total_Trip_miles"),  sum("Total_trip_duration").as("Total_trip_duration"), mean("Avg_Trip_fare").as("avg_trip_fare"),
        mean("Avg_Trip_miles").as("avg_trip_miles"),  mean("Avg_trip_duration").as("avg_trip_duration"))

    val monthly_avg_trip_count = monthlySummaryNormalAvgDf.select("trip_count").agg(avg("trip_count")).head.getDouble(0)

    val monthlySummaryDf = monthlySummaryNormalAvgDf.select(col("year"), col("month"),
      col("trip_count").as("Total_Trip_count"), col("Total_Trip_fare"), col("Total_Trip_miles"), round(col("Total_trip_duration"),2).as("Total_trip_duration"),
      (round(col("avg_trip_fare") * col("trip_count") / monthly_avg_trip_count,4)).as("Avg_Trip_fare"), (round(col("avg_trip_miles") * col("trip_count") / monthly_avg_trip_count,4)).as("Avg_Trip_miles"),
      (round(col("avg_trip_duration") * col("trip_count") / monthly_avg_trip_count,4)).as("Avg_trip_duration")).withColumn("month_no",tripId)

    val monthlySummary = monthlySummaryDf.select(col("month_no"), monthStartDate(col("month"), col("year")).as("date_from"), monthEndDate(col("month"), col("year")).as("date_to"),
      col("Total_Trip_count"), col("Total_Trip_fare"), col("Total_Trip_miles"), col("Total_trip_duration"), col("Avg_Trip_fare"),
      col("Avg_Trip_miles"), col("Avg_trip_duration"))

    // *****************************************loading output to hive tables************************************

    dailySummaryDf.write.mode("overwrite").format("orc").saveAsTable("edureka_735821.dailySummary")
    weeklySummary.write.mode("overwrite").format("orc").saveAsTable("edureka_735821.weeklySummary")
    monthlySummary.write.mode("overwrite").format("orc").saveAsTable("edureka_735821.monthlySummary")
  }
}
