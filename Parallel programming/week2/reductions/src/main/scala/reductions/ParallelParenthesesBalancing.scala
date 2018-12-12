package reductions

import scala.annotation._
import org.scalameter._
import common._

import scala.collection.JavaConverters._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
    println(s"Sequential was $seqResult and Parallel was $parResult")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    if (chars.isEmpty) true
    else {
      var i = 0
      var balanced = true
      var balance = 0
      while(i < chars.length && balanced) {
        if (chars(i) == '(')
          balance = balance + 1
        else if (chars(i) == ')') {
          balance = balance - 1
          if (balance < 0 )  balanced=false
        }
        i = i + 1
      }
      balance == 0  && balanced
    }
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int, arg1: Int, arg2: Int) : (Int,Int) = {
      // openNotMatched -> # of ( not matched; closedNotMatched -> # of ) not matched
      var i = idx
      var openNotMatched = arg1
      var closedNotMatched = arg2
      while(i < until){
        if (chars(i) == '(') openNotMatched = openNotMatched + 1
        else if (chars(i) == ')') {
          if(openNotMatched > 0) openNotMatched = openNotMatched -1
          else closedNotMatched = closedNotMatched + 1
        }
        i = i + 1
      }
      (openNotMatched, closedNotMatched)
    }

    def reduce(from: Int, until: Int) : (Int, Int) = {
      if (until - from <= threshold) traverse(from, until, 0, 0)
      else {
        val mid = from + (until - from) / 2
        val (l, r) = parallel(reduce(from, mid), reduce(mid, until))
        val matched = scala.math.min(l._1, r._2)
        (l._1 + r._1 - matched, l._2 + r._2 - matched)
      }
    }

    reduce(0, chars.length) == (0,0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
