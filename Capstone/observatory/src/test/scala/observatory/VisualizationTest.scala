package observatory


import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

trait VisualizationTest extends FunSuite with Checkers {

//  test("predictTemperature") {
//
//    val output = Extraction.locateTemperatures(1975, "/stations.csv", "/1975.csv")
//
//    val v = Visualization.predictTemperature(Extraction.locationYearlyAverageRecords(output), Location(70.933, 008.667))
//    Thread.sleep(600000)
//  }

  test("exceeding the greatest value of a color scale should return the color associated with the greatest value") {
    val colors = Seq((0.0,Color(0,0,0)), (1.0,Color(0,0,0)), (2.0,Color(0,0,0)), (3.0,Color(1,1,1)))
    assert(Visualization.interpolateColor(colors, 10.0) == Color(1,1,1))
  }

  test("not reaching the minimum value of a color scale should return the color associated with the greatest value") {
    val colors = Seq((0.0,Color(1,1,1)), (1.0,Color(0,0,0)), (2.0,Color(0,0,0)), (3.0,Color(0,0,0)))
    assert(Visualization.interpolateColor(colors, -10.0) == Color(1,1,1))
  }

  test("mytest") {
    val scale = List((-87.78412284356342, Color(255,0,0)), (-1.0, Color(0,0,255)))
    assert(Visualization.interpolateColor(scale, 9.0) == Color(0,0,255))
  }

}
