package btree

import java.util.concurrent.ThreadLocalRandom

import org.scalatest.FlatSpec

class DeletionSpec extends FlatSpec {

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
      /*val data = scala.collection.mutable.ArrayBuffer[Int]()

      for(i<-0 until 24){
        val k = rand.nextInt(1, MAX_VALUE)

        if(index.insert(k, k)){
          data += k
        }
      }*/

      val data = scala.collection.mutable.ArrayBuffer(44, 90, 118, 125, 153, 199, 275, 296, 304, 316, 324,
        412, 482, 559, 560, 639, 671, 740, 743, 797, 818, 833, 878, 894)

      for(i<-0 until data.size){
        val k = data(i)
        index.insert(k, k)
      }

      index.prettyPrint()

      val removed = scala.collection.mutable.ArrayBuffer[Int]()

      for(i<-0 until data.size){
        val pos = rand.nextInt(0, data.size)
        val k = data(pos)

        if(!removed.contains(k)){

          index.remove(k)
          data -= k

          removed += k
        }
      }

      println(s"\ndata: ${data}\n")
      println(s"\nremoved: ${removed}\n")

      val sorted = data.sorted
      val bsorted = index.inOrder

      println(s"\nDATA: ${sorted}\n")
      println(s"\nINDEX: ${bsorted}\n")

      index.prettyPrint()

      assert(bsorted.equals(sorted))

    }

  }

}
