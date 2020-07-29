/*
spark2-submit --class batch.VideoPlayCSVBatch \
--packages com.datastax.spark:spark-cassandra-connector_2.11:2.0.12 \
--conf spark.cassandra.auth.username=... \
--conf spark.cassandra.auth.password=... \
target/scala-2.11/mid-project-2_2.11-0.1.jar VideoPlayLake/Inbound/csv videos.video_play_stg
 */

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
