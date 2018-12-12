package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

/**
  * 2nd milestone: basic visualization
  */
object Visualization {

  // Exponent of distance in the Inverse distance weighting computing
  val p: Double = 2.0

  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature = {

    def weight(dist: Double): Double = 1 / math.pow(dist, p)

    var weightAccum: Double = 0
    var tempAccum: Double   = 0

    // Check if there is any station close than 1km to use its temp. Otherwise use the weighted temp
    val computedDist = temperatures.map({case (loc, temp) => (loc.distance(location), temp)})

    val minDist = computedDist.filter({case (dist, _) => dist < 1})

    if(minDist.nonEmpty) minDist.head._2
    else {
      computedDist
        .foreach({
          case (dist, temp) =>
            val w: Double = weight(dist)
            weightAccum += w
            tempAccum   += w * temp
        })

      tempAccum / weightAccum
    }
  }

  def predictTemperatureCurried(temperatures: Iterable[(Location, Temperature)])(loc: Location): Temperature = {
    predictTemperature(temperatures, loc)
  }


  /**
    * @param points Pairs containing a value and its associated color
    * @param value The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Temperature, Color)], value: Temperature): Color = {
    // Find the above and below points for value
    var below: (Temperature, Color) = (Double.NegativeInfinity, Color(0,0,0))
    var above: (Temperature, Color) = (Double.PositiveInfinity, Color(0,0,0))

    var exactTemperature = false

    points.takeWhile(_ => !exactTemperature).foreach({
      case (t, c) =>
        if( t == value) { exactTemperature = true; below = (t,c) }
        else if(t < value && t > below._1) below = (t, c)
        else if(t > value && t < above._1) above = (t, c)
    })

    if (exactTemperature)
      below._2
    else{
      if(below._1 == Double.NegativeInfinity) above._2
      else if (above._1 == Double.PositiveInfinity) below._2
      else {
        val weight = (value - below._1) / (above._1 - below._1)
        Color((below._2.red   * (1 - weight) + above._2.red   * weight).round.toInt,
              (below._2.green * (1 - weight) + above._2.green * weight).round.toInt,
              (below._2.blue  * (1 - weight) + above._2.blue  * weight).round.toInt)
      }
    }
  }

  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {

    // Image size
    val height: Int = 180
    val width:  Int = 360
    val alpha:  Int = 255

    def toLocation(coord: (Int, Int)): Location = {
      val lon =  coord._1 - 180
      val lat =  90 - coord._2
      Location(lat, lon)
    }

    val spark = Main.getSparkSession

    visualize(colors, toLocation, predictTemperatureCurried(temperatures)(_), width, height, alpha, spark)
  }

  /**
    *
    * @param colors       Color scale
    * @param toLocation   Function to transform pixel coordinates to (lat,long)
    * @param width        Width of the image to generate
    * @param height       Height of the image to generate
    * @param alpha        Alpha (transparency) of the pixels of the generated image
    * @param spark        SparkSession object
    * @return             A width×height image where each pixel shows the predicted temperature at its location
    */
  def visualize(colors: Iterable[(Temperature, Color)],
                toLocation: ((Int, Int)) => Location,
                predictTemperature: Location => Temperature,
                width: Int, height: Int, alpha: Int,
                spark: SparkSession): Image = {

    import spark.implicits._

    // Generate coordinates row-wise, to comply with expected order by Image
    val coords: Seq[(Int, Int)] = for {
      y <- 0 until height
      x <- 0 until width
    } yield (x, y)

    val coordsDf = coords.toDF()

    val pixels = coordsDf
      .map {
        case Row(x: Int, y: Int) =>
          val temp = predictTemperature(toLocation((x, y)))
          val col = interpolateColor(colors, temp)
          Pixel(col.red, col.green, col.blue, alpha)
      }
      .collect()

    Image(width, height, pixels)
  }

}

