package observatory

import com.sksamuel.scrimage.Image
import observatory.Visualization.{predictTemperatureCurried, visualize}
import org.apache.spark.sql.{Dataset, Encoder, Row, SparkSession}


/**
  * 3rd milestone: interactive visualization
  */
object Interaction {


  /**
    * @param tile Tile coordinates
    * @return The latitude and longitude of the top-left corner of the tile, as per http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
    */
  def tileLocation(tile: Tile): Location = tile.toLocation

  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @param tile Tile coordinates
    * @return A 256×256 image showing the contents of the given tile
    */
  def tile(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)], tile: Tile): Image = {

    val height: Int = 256
    val width:  Int = 256
    val alpha:  Int = 127

    def toLocation(coords: (Int, Int)): Location = {
      // Tile offset of this tile in the zoom+8 coordinate system
      val x0 = (1  << 8) * tile.x
      val y0 = (1  << 8) * tile.y
      tileLocation(Tile(coords._1 + x0, coords._2 + y0, tile.zoom + 8))
    }

    val spark = Main.getSparkSession
    visualize(colors, toLocation, predictTemperatureCurried(temperatures)(_), width, height, alpha, spark)
  }

  /**
    * Generates all the tiles for zoom levels 0 to 3 (included), for all the given years.
    * @param yearlyData Sequence of (year, data), where `data` is some data associated with
    *                   `year`. The type of `data` can be anything.
    * @param generateImage Function that generates an image given a year, a zoom level, the x and
    *                      y coordinates of the tile and the data to build the image from
    */
  def generateTiles[Data](yearlyData: Iterable[(Year, Data)], generateImage: (Year, Tile, Data) => Unit): Unit= {

    val spark = Main.getSparkSession

    val coords: Seq[Tile] = for {
      zoom <- 0 to 3
      y <- 0 until 1 << zoom
      x <- 0 until 1 << zoom
    } yield Tile(x, y, zoom)

    generateTiles(yearlyData, generateImage, coords, spark)
  }

  def generateTiles[Data](yearlyData: Iterable[(Year, Data)],
                         generateImage: (Year, Tile, Data) => Unit,
                         coords: Seq[Tile],
                         spark: SparkSession): Unit = {

    import spark.implicits._

    val coordsDf      = coords.toDF()
    val yearlyDataMap = yearlyData.toMap
    val yearDf        = yearlyDataMap.keys.toSeq.toDF()

    yearDf
      .crossJoin(coordsDf)
      .foreach((row: Row) => row match {
        case Row(year: Int, x: Int, y:Int, zoom:Int) => generateImage(year, Tile(x,y,zoom), yearlyDataMap(year))
      })
  }

}
