package quickcheck

import common._


import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  lazy val genHeap: Gen[H] = oneOf(
    const(empty),
    for {
      v <- arbitrary[A]
      h <- genHeap
    } yield insert(v, h)
  )
  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

  property("gen1") = forAll { (h: H) =>
    val m = if (isEmpty(h)) 0 else findMin(h)
    findMin(insert(m, h)) == m
  }

  property("insert") = forAll { (a: A, b: A) =>
    val min = if (a < b) a else b
    findMin(insert(b, insert(a, empty))) == min
  }

  property("delete on empty") = forAll { (a: A) =>
    isEmpty(deleteMin(insert(a, empty)))
  }

  property("delete min") = forAll { (h:H) =>
    isEmpty(h) || isEmpty(deleteMin(h)) || findMin(h) != findMin(deleteMin(h))
  }

  property("min") = forAll { (h1: H, h2: H) =>
    val h3 = meld(h1, h2)
    val h3Min = if (isEmpty(h3)) null else findMin(h3)
    val h1Min = if (isEmpty(h1)) null else findMin(h1)
    val h2Min = if (isEmpty(h2)) null else findMin(h2)
    (h3Min == h1Min) || (h3Min == h2Min)
  }

  property("ordered") = forAll { (h: H) =>
    def orderedHeap(h: H): List[A] =  {
      if (isEmpty(h)) Nil
      else findMin(h) :: orderedHeap(deleteMin(h))
    }
    isEmpty(h) || isEmpty(deleteMin(h)) || orderedHeap(h).sliding(2).forall{ case List(x, y) => x <= y }
  }

}
