package observatory

import java.time.LocalDate
import java.nio.file.Paths

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._



/**
  * 1st milestone: data extraction
  */
object Extraction {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  val stationSchema: StructType = StructType(
    Seq(
      StructField("STN", IntegerType, false),
      StructField("WBAN", IntegerType, false),
      StructField("Lat", DoubleType, false),
      StructField("Long", DoubleType, false)
    ))

  val tempSchema: StructType =StructType(
    Seq(
      StructField("STN", IntegerType, false),
      StructField("WBAN", IntegerType, false),
      StructField("Month", IntegerType, false),
      StructField("Day", IntegerType, false),
      StructField("Temp", DoubleType, false)
    ))

  /** @return The filesystem path of the given resource */
  def fsPath(resource: String): String = Paths.get(getClass.getResource(resource).toURI).toString

  /**
    *
    * @param resource  Name of the csv file to load (relative to resources dir)
    * @param schema    Schema to use for the data
    * @param spark     SparkSession object
    * @return          Dataframe with csv data
    */
  def read(resource: String, schema: StructType, spark: SparkSession): DataFrame =
    spark.read
      .schema(schema)
      .csv(fsPath(resource))


  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String,
                         temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {


    val spark = Main.getSparkSession
    val stationsDf = read(stationsFile, stationSchema, spark).na.fill(0, Seq("STM", "WBAN"))
    val tempDf = read(temperaturesFile, tempSchema, spark).na.fill(0, Seq("STM", "WBAN"))

    locateTemperatures(year, stationsDf, tempDf).collect().toSeq
  }


  def locateTemperatures(year: Year, stations: DataFrame,
                         temperatures: DataFrame): RDD[(LocalDate, Location, Temperature)] = {

    def toCelsius(f: Double): Double = (f - 32.0) * (5.0/9.0)

    // Work with RDD due to Spark being unable to serialize LocalDate class
    stations
      .na.drop(Seq("Lat", "Long")) // Retain only stations with gps info
      .join(temperatures, Seq("STN", "WBAN"))
      .rdd
      .map(row => (LocalDate.of(year, row.getAs[Int]("Month"), row.getAs[Int]("Day")),
                  Location(row.getAs[Double]("Lat"), row.getAs[Double]("Long")),
                  toCelsius(row.getAs[Temperature]("Temp")))
      )
  }


  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    val spark = Main.getSparkSession
    val recordsRDD = spark
                      .sparkContext
                      .parallelize(records.toSeq)
    locationYearlyAverageRecords(recordsRDD).collect()
  }

  def locationYearlyAverageRecords(records: RDD[(LocalDate, Location, Temperature)]): RDD[(Location, Temperature)] = {
    records
      .map(t => (t._2, (t._3, 1.0)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues({case (sumTemp, n) => sumTemp/n})
  }

}
