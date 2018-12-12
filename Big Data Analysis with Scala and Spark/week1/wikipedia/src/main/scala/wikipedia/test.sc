import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


val conf: SparkConf = new SparkConf()
  .setMaster("local[*]")
  .setAppName("Wikipedia")
  .set("spark.executor.memory", "1gb")
val sc: SparkContext = new SparkContext(conf)

case class WikipediaArticle(title: String, text: String) {
  /**
    * @return Whether the text of this article mentions `lang` or not
    * @param lang Language to look for (e.g. "Scala")
    */
  def mentionsLanguage(lang: String): Boolean = text.split(' ').contains(lang)
}


val parse = (line: String) => {
  val subs = "</title><text>"
  val i = line.indexOf(subs)
  val title = line.substring(14, i)
  val text  = line.substring(i + subs.length, line.length-16)
  WikipediaArticle(title, text)
}


def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] = {
  sc.parallelize(langs).cartesian(rdd).groupByKey()
}




val langs = List(
  "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
  "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

//val wikiRdd: RDD[WikipediaArticle] = sc.textFile("src/main/resources/wikipedia/wikipedia.dat").map(parse)
//val wikiRdd: RDD[String] = sc.textFile("src/main/resources/wikipedia/wikipedia.dat")





