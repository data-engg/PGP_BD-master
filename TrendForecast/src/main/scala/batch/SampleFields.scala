package batch

import utilities.Utils

object SampleFields {

  def main (args : Array[String]) : Unit = {

    // ----------------------- Input validation -----------------------

    if (args.length != 3){
      println(" Usage : <jar> <source path> <target path <format>")
      System.exit(1)
    } else {
      if (args(2).trim != "csv" & args(2).trim != "xml") {
        println( "Valid input formats are csv and xml")
        System.exit(1)
      }
    }

    // ----------------------- Spark session -----------------------
    val spark = Utils.get_spark_session("Sample 20% records from dataset")

    // ----------------------- Sample data -----------------------

    val input_rdd =
    spark.sparkContext.textFile(args(0)).sample(false, 0.2)

    if (args(2) == "csv"){
      input_rdd
        .coalesce(1)
        .saveAsTextFile(args(1)+"/csv/")
    }

    if (args(2) == "xml"){
      input_rdd
        .coalesce(1)
        .saveAsTextFile(args(1)+"/xml/")
    }
  }

}
