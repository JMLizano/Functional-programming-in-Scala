package observatory

import org.junit.runner.RunWith
import org.scalatest._
import Matchers._
import org.scalatest.junit.JUnitRunner
import java.time.LocalDate


trait ExtractionTest extends FunSuite {

  test("locateTemperaturesRDD") {
    val expectedOutput = Seq(
      (LocalDate.of(2015, 8, 11), Location(37.35, -78.433), 27.3),
      (LocalDate.of(2015, 12, 6), Location(37.358, -78.438), 0.0),
      (LocalDate.of(2015, 1, 29), Location(37.358, -78.438), 2.000000000000001)
    )

    val output = Extraction.locateTemperatures(2015, "/stationsTest.csv", "/2018.csv")

    expectedOutput should contain theSameElementsAs output
  }

  test("locationYearlyAverageRecords") {
    val input = Seq(
      (LocalDate.of(2015, 8, 11), Location(37.35, -78.433), 27.3),
      (LocalDate.of(2015, 12, 6), Location(37.358, -78.438), 0.0),
      (LocalDate.of(2015, 1, 29), Location(37.358, -78.438), 2.0)
    )
    val expectedOutput = Seq(
      (Location(37.35, -78.433), 27.3),
      (Location(37.358, -78.438), 1.0)
    )
    val output = Extraction.locationYearlyAverageRecords(input)
    expectedOutput should contain theSameElementsAs output
  }

  
}