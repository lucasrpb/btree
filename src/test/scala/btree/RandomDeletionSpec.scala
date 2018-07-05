package btree

import java.util.concurrent.ThreadLocalRandom

import org.scalatest.FlatSpec

class RandomDeletionSpec extends FlatSpec {

  "btree data" should "be equal test data" in {

    implicit val comp = new Ordering[Int] {
      override def compare(x: Int, y: Int): Int = x - y
    }

    val rand = ThreadLocalRandom.current()

    val iterations = 100

    for(j<-0 until iterations){

      val MAX_VALUE = 1000
      val n = 10000

      val DATA_ORDER = rand.nextInt(2, 10)
      val META_ORDER = rand.nextInt(2, 10)

      val index = new Index[String, Int, Int](DATA_ORDER, META_ORDER)
      val data = scala.collection.mutable.ArrayBuffer[Int]()

      for(i<-0 until n){

        rand.nextInt match {
          case n if n % 2 == 0 =>

            val k = rand.nextInt(1, MAX_VALUE)

            index.insert(k, k)

            if(!data.contains(k)){
              data += k
            }

          case n if n % 3 == 0 && data.size > 0 =>

            val pos = rand.nextInt(0, data.size)
            val k = data(pos)

            index.remove(k)
            data -= k

          case _ =>
        }

      }

      val sorted = data.sorted
      val bsorted = index.inOrder

      println(s"\nDATA: ${sorted}\n")
      println(s"\nINDEX: ${bsorted}\n")

      //index.prettyPrint()

      assert(bsorted.equals(sorted))

    }

  }

}
