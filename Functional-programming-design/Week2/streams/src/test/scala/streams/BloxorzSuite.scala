package streams

import org.scalatest.FunSuite

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import Bloxorz._

@RunWith(classOf[JUnitRunner])
class BloxorzSuite extends FunSuite {

  trait SolutionChecker extends GameDef with Solver with StringParserTerrain {
    /**
     * This method applies a list of moves `ls` to the block at position
     * `startPos`. This can be used to verify if a certain list of moves
     * is a valid solution, i.e. leads to the goal.
     */
    def solve(ls: List[Move]): Block =
      ls.foldLeft(startBlock) { case (block, move) =>
        require(block.isLegal) // The solution must always lead to legal blocks
        move match {
          case Left => block.left
          case Right => block.right
          case Up => block.up
          case Down => block.down
        }
    }
  }

  trait Level1 extends SolutionChecker {
      /* terrain for level 1*/

    val level =
    """ooo-------
      |oSoooo----
      |ooooooooo-
      |-ooooooooo
      |-----ooToo
      |------ooo-""".stripMargin

    val optsolution = List(Right, Right, Down, Right, Right, Right, Down)
  }


	test("terrain function level 1") {
    new Level1 {
      assert(terrain(Pos(0,0)), "0,0")
      assert(terrain(Pos(1,1)), "1,1") // start
      assert(terrain(Pos(4,7)), "4,7") // goal
      assert(terrain(Pos(5,8)), "5,8")
      assert(!terrain(Pos(5,9)), "5,9")
      assert(terrain(Pos(4,9)), "4,9")
      assert(!terrain(Pos(6,8)), "6,8")
      assert(!terrain(Pos(4,11)), "4,11")
      assert(!terrain(Pos(-1,0)), "-1,0")
      assert(!terrain(Pos(0,-1)), "0,-1")
    }
  }

	test("findChar level 1") {
    new Level1 {
      assert(startPos == Pos(1,1))
    }
  }


//  test("neighborsWithHistory") {
//    new Level1 {
//      println("StartPos")
//      neighborsWithHistory(Block(startPos, startPos), Nil).foreach(println)
//      println("3,1")
//      neighborsWithHistory(Block(Pos(3,1), Pos(3,1)), Nil).foreach(println)
//      println("(2,2), (2,3)")
//      neighborsWithHistory(Block(Pos(2,2), Pos(2,3)), Nil).foreach(println)
//    }
//  }

//  test("new neighbors only") {
//    new Level1 {
//      println("StartPos")
//      val n = neighborsWithHistory(Block(Pos(2,2), Pos(2,3)), Nil)
//      newNeighborsOnly(n, Set())
//      newNeighborsOnly(n, Set(Block(Pos(3,2),Pos(3,3)), Block(Pos(1,2),Pos(1,3))))
//    }
//  }

  test("from") {
    new Level1 {
      from(Stream(Tuple2(startBlock, Nil)), Set()).take(5)
    }
  }


  test("optimal solution for level 1") {
    new Level1 {
      println("SOLUTION")
      println(solution)
      assert(solve(solution) == Block(goal, goal))
    }
  }

	test("optimal solution length for level 1") {
    new Level1 {
      assert(solution.length == optsolution.length)
    }
  }

}
