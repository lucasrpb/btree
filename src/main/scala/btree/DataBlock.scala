package btree

import scala.reflect.ClassTag

class DataBlock[T: ClassTag, K: ClassTag, V: ClassTag](override val id: T,
                                                       override val MIN: Int,
                                                       override val MAX: Int)
  extends Block[T, K, V]{

  val keys = Array.ofDim[(K, V)](MAX)
  var next: Option[DataBlock[T, K, V]] = None
  var prev: Option[DataBlock[T, K, V]] = None

  override def isFull() = size == MAX

  def find(key: K, start: Int = 0, end: Int = size - 1)(implicit comp: Ordering[K]): (Boolean, Int) = {
    if(start > end) return false -> start

    val pos = start + (end - start)/2
    val k = keys(pos)._1
    val c = comp.compare(key, k)

    if(c == 0) return true -> pos
    if(c < 0) return find(key, start, pos - 1)(comp)

    find(key, pos + 1, end)
  }

  def insert(idx: Int, key: (K, V)): Boolean = {
    for(i<-(size until idx by -1)){
      keys(i) = keys(i-1)
    }

    keys(idx) = key

    size += 1

    true
  }

  def insert(key: (K, V))(implicit comp: Ordering[K]): Boolean = {
    if(isFull()) return false

    val r = find(key._1)(comp)

    if(r._1) return false

    insert(r._2, key)
  }

  def remove(idx: Int): Boolean = {
    size -= 1

    for(i<-idx until size){
      keys(i) = keys(i+1)
    }

    true
  }

  def removeByKey(key: K)(implicit comp: Ordering[K]): Boolean = {
    val r = find(key)(comp)

    if(!r._1) return false
    remove(r._2)
  }

  def removeAndGet(idx: Int): (K, V) = {
    val key = keys(idx)
    remove(idx)
    key
  }

  def left: Option[DataBlock[T, K, V]] = {
    parent match {
      case None => None
      case Some(parent) =>
        val idx = pos - 1
        if(idx >= 0) Some(parent.pointers(idx).asInstanceOf[DataBlock[T, K, V]]) else None
    }
  }

  def right: Option[DataBlock[T, K, V]] = {
    parent match {
      case None => None
      case Some(parent) =>
        val idx = pos + 1

        if(idx <= parent.size) Some(parent.pointers(idx).asInstanceOf[DataBlock[T, K, V]]) else None

    }
  }

  def inOrder: Seq[K] = keys.slice(0, size).map(_._1)

  override def toString = s"""[${keys.slice(0, size).map(_._1).mkString(",")}]"""

}
