package btree

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom

import org.scalatest.FlatSpec

class DataBlockSpec extends FlatSpec {

  "data block data must" should "be equal to test data" in {

    implicit val comp = new Ordering[Int] {
      override def compare(x: Int, y: Int): Int = x - y
    }

    val MAX_VALUE = 1000000
    val n = 100

    val ORDER = 1000
    val MIN = ORDER - 1
    val MAX = ORDER*2 - 1

    val block = new DataBlock[String, Int, Int](UUID.randomUUID.toString, MIN, MAX)
    val data = scala.collection.mutable.ArrayBuffer[Int]()

    val rand = ThreadLocalRandom.current()

    for(i<-0 until n){

      rand.nextInt match {
        case n if n % 2 == 0 =>

          val k = rand.nextInt(1, MAX_VALUE)

          println(s"\nINSERTING ${k}...\n")

          if(block.insert(k -> k)){
            data += k
          }

        case n if data.size > 0 && n % 3 == 0 =>

          val pos = rand.nextInt(0, data.size)
          val k = data(pos)

          println(s"\nREMOVING ${k}...\n")

          if(block.removeByKey(k)){
            data -= k
          }

        case _ =>

      }

    }

    val sorted = data.sorted
    val bsorted = block.inOrder

    println(s"\nDATA: ${sorted}\n")
    println(s"\nBLOCK: ${bsorted}\n")

    assert(bsorted.equals(sorted))

  }

}
