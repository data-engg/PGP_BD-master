package batch
import utilities._

object VideoPlayCSVBatch {

    def main(args : Array[String]) : Unit ={

      // ----------------------- Spark session -----------------------
      val spark = Utils.get_spark_session("CSV batch processing for enrichment")

      // ----------------------- load data to lookups -----------------------

      Utils.channelGeocd(spark)
      Utils.videoCreator(spark)

      // ----------------------- load data in dataframe -----------------------
      val input_df = spark.read.format("csv").
        schema(Utils.getSchema()).
        options(Map("header" -> "true")).
        load(args(0))

      // ----------------------- Data validation and enrichment -----------------------

      val null_replaced_df = Processing.null_replacement(input_df)

      val enriched = Processing.enrichment(null_replaced_df)

      // ----------------------- Lake ingestion -----------------------

      Processing.load(enriched, args(1))
    }
}
