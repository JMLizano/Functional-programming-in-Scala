package stackoverflow

import org.scalatest.{FunSuite, BeforeAndAfterAll}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.io.File

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {


  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    override def langSpread = 50000
    override def kmeansKernels = 45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120
  }

  override def afterAll(): Unit = {
    import StackOverflow._
    sc.stop()
  }

  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  test("groupedPostings") {
    import StackOverflow._
    val posts = List(
      Posting(1, 1, None, None, 10, None),
      Posting(1, 2, None, None, 11, None),
      Posting(2, 3, None, Some(1), 10, None),
      Posting(2, 4, None, Some(2), 10, None),
      Posting(2, 5, None, Some(2), 10, None)
    )
    val grouped = testObject.groupedPostings(sc.parallelize(posts)).collectAsMap()
    assert(grouped.keys.size == 2)
    assert(grouped(1).toList == List((Posting(1, 1, None, None, 10, None), Posting(2, 3, None, Some(1), 10, None))))
    assert(grouped(2).toList == List((Posting(1, 2, None, None, 11, None), Posting(2, 4, None, Some(2), 10, None)),
                                     (Posting(1, 2, None, None, 11, None), Posting(2, 5, None, Some(2), 10, None))
                                    )
    )
  }

  test("scoredPostings") {
    import StackOverflow._
    val posts = List(
      Posting(1, 1, None, None, 10, None),
      Posting(1, 2, None, None, 11, None),
      Posting(2, 3, None, Some(1), 10, None),
      Posting(2, 4, None, Some(2), 11, None),
      Posting(2, 5, None, Some(2), 12, None)
    )
    val grouped = testObject.groupedPostings(sc.parallelize(posts))
    val scored = testObject.scoredPostings(grouped).collectAsMap()

    assert(scored.keys.size == 2)
    assert(scored(Posting(1, 1, None, None, 10, None)) == 10)
    assert(scored(Posting(1, 2, None, None, 11, None)) == 12)
  }

  test("vectorPostings") {
    import StackOverflow._
    val posts = List(
      Posting(1, 1, None, None, 10,  Some("JavaScript")),
      Posting(1, 2, None, None, 11, Some("Java")),
      Posting(2, 3, None, Some(1), 10, None),
      Posting(2, 4, None, Some(2), 11, None),
      Posting(2, 5, None, Some(2), 12, None)
    )
    val grouped = testObject.groupedPostings(sc.parallelize(posts))
    val scored  = testObject.scoredPostings(grouped)
    val vectors = testObject.vectorPostings(scored).collectAsMap()

    assert(vectors.keys.size == 2)
    assert(vectors(0) == 10)
    assert(vectors(50000) == 12)
  }

  test("clusterResults"){
    import StackOverflow._
    val centers = Array((0,0), (100000, 0))
    val rdd = sc.parallelize(List(
      (0, 1000),
      (0, 23),
      (0, 234),
      (0, 0),
      (0, 1),
      (0, 1),
      (50000, 2),
      (50000, 10),
      (100000, 2),
      (100000, 5),
      (100000, 10),
      (200000, 100)  ))
    testObject.printResults(testObject.clusterResults(centers, rdd))
  }

}
