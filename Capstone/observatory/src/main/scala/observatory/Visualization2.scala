package observatory

import com.sksamuel.scrimage.Image
import observatory.Interaction.tileLocation
import observatory.Visualization.visualize

/**
  * 5th milestone: value-added information visualization
  */
object Visualization2 {

  /**
    * @param point (x, y) coordinates of a point in the grid cell
    * @param d00 Top-left value
    * @param d01 Bottom-left value
    * @param d10 Top-right value
    * @param d11 Bottom-right value
    * @return A guess of the value at (x, y) based on the four known values, using bilinear interpolation
    *         See https://en.wikipedia.org/wiki/Bilinear_interpolation#Unit_Square
    */
  def bilinearInterpolation(
    point: CellPoint,
    d00: Temperature,
    d01: Temperature,
    d10: Temperature,
    d11: Temperature
  ): Temperature = {
    (d00 * (1.0 - point.x) * (1.0 - point.y)) + (d10 * point.x * (1.0 - point.y)) + (d01 * (1.0 - point.x) * point.y) + (d11 * point.x * point.y)
  }

  def predictTemperature(grid: GridLocation => Temperature)(loc: Location): Temperature = {
    // For each pixel (x,y) get for rounding corners and interpolate
    val lonFloor = loc.lon.floor.toInt
    val lonCeil = loc.lon.ceil.toInt
    val latFloor = loc.lat.floor.toInt
    val latCeil = loc.lat.ceil.toInt

    val d00 = grid(GridLocation(latFloor, lonFloor))
    val d01 = grid(GridLocation(latCeil, lonFloor))
    val d10 = grid(GridLocation(latFloor, lonCeil))
    val d11 = grid(GridLocation(latCeil, lonCeil))

    val xDelta = loc.lon - lonFloor
    val yDelta = loc.lat - latFloor

    bilinearInterpolation(CellPoint(xDelta, yDelta), d00, d01, d10, d11)
  }
  /**
    * @param grid Grid to visualize
    * @param colors Color scale to use
    * @param tile Tile coordinates to visualize
    * @return The image of the tile at (x, y, zoom) showing the grid using the given color scale
    */
  def visualizeGrid(
    grid: GridLocation => Temperature,
    colors: Iterable[(Temperature, Color)],
    tile: Tile
  ): Image = {

    val height: Int = 256
    val width:  Int = 256
    val alpha:  Int = 127
    val spark = Main.getSparkSession

    def toLocation(coords: (Int, Int)): Location = {
      // Tile offset of this tile in the zoom+8 coordinate system
      val x0 = (1  << 8) * tile.x
      val y0 = (1  << 8) * tile.y
      tileLocation(Tile(coords._1 + x0, coords._2 + y0, tile.zoom + 8))
    }

    visualize(colors, toLocation, predictTemperature(grid)(_), width, height, alpha, spark)
  }

}
