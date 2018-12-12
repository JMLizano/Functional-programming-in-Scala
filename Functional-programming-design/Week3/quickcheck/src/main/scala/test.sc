
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Gen.{const, oneOf}
import quickcheck.{Heap, BinomialHeap, IntHeap, Bogus4BinomialHeap}


trait gen extends  IntHeap   {
  
  lazy val genHeap: Gen[H] = oneOf(
    const(empty),
    for {
      v <- arbitrary[A]
      h <- genHeap
    } yield insert(v, h)
  )
  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

  def orderedHeap(h: H): List[A] =  {
    if (isEmpty(h)) Nil
    else findMin(h) :: orderedHeap(deleteMin(h))
  }
}

class okHeap  extends BinomialHeap with gen
class bugHeap4 extends Bogus4BinomialHeap with gen





val bugHeap = new bugHeap4
val h = List(bugHeap.Node(1,0,List()), bugHeap.Node(-1100974088,1,List(bugHeap.Node(2147483647,0,List()))))
bugHeap.findMin(h)
bugHeap.deleteMin(h)
bugHeap.orderedHeap(h)
bugHeap.isEmpty(h) || bugHeap.isEmpty(bugHeap.deleteMin(h)) || bugHeap.orderedHeap(h).sliding(2).forall{ case List(x, y) => x <= y }


val kheap = new okHeap
val hok = List(kheap.Node(1,0,List()), kheap.Node(-1100974088,1,List(kheap.Node(2147483647,0,List()))))
val hok2 = List(kheap.Node(0,1,List(kheap.Node(0,0,List()))))
kheap.findMin(hok)
kheap.deleteMin(hok)
kheap.orderedHeap(hok)
kheap.isEmpty(hok) || kheap.isEmpty(kheap.deleteMin(hok)) || kheap.orderedHeap(hok).sliding(2).forall{ case List(x, y) => x <= y }
hok2.length < 2 || kheap.orderedHeap(hok2).sliding(2).forall{ case List(x, y) => x <= y }
