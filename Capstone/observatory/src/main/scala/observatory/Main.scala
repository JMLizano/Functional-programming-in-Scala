package observatory

import org.apache.spark.sql.SparkSession


object Main extends App {

  def getSparkSession: SparkSession =
    SparkSession
      .builder()
      .appName("Capstone")
      .config("spark.master", "local[*]")
      .getOrCreate()

}
