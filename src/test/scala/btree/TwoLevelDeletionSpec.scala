package btree

import java.util.concurrent.ThreadLocalRandom

import org.scalatest.FlatSpec

class TwoLevelDeletionSpec extends FlatSpec {

  "btree data" should "be equal test data" in {

    implicit val comp = new Ordering[Int] {
      override def compare(x: Int, y: Int): Int = x - y
    }
    
    val rand = ThreadLocalRandom.current()

    val iterations = 1

    for(j<-0 until iterations){

      val MAX_VALUE = 1000
      val n = 10000

      //val DATA_ORDER = rand.nextInt(2, 10)
      //val META_ORDER = rand.nextInt(2, 10)

      val DATA_ORDER = 2
      val META_ORDER = 2

      val index = new Index[String, Int, Int](DATA_ORDER, META_ORDER)
      //val data = scala.collection.mutable.ArrayBuffer[Int]()

      val data = scala.collection.mutable.ArrayBuffer(700, 888, 196, 371, 42, 317, 471, 403, 589)

      for(i<-0 until data.size){
        val k = data(i)
        index.insert(k, k)
      }

      val remove = Seq(700, 471, 403, 317, 196, 888, 42, 589, 371)

      for(i<-0 until remove.size){

        val k = remove(i)
        index.remove(k)
        data -= k

      }

      println(s"\ndata: ${data}\n")

      val sorted = data.sorted
      val bsorted = index.inOrder

      println(s"\nDATA: ${sorted}\n")
      println(s"\nINDEX: ${bsorted}\n")

      assert(bsorted.equals(sorted))

    }

  }

}
