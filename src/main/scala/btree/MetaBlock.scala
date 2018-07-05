package btree

import scala.reflect.ClassTag

class MetaBlock[T: ClassTag, K: ClassTag, V: ClassTag](val id: String,
                                                       override val MIN: Int,
                                                       override val MAX: Int)
                                                      /*(implicit val comp: Ordering[K])*/
  extends Block[T, K, V] {

  val keys = Array.ofDim[K](MAX)
  val pointers = Array.ofDim[Block[T, K, V]](MAX + 1)

  def find(key: K, start: Int = 0, end: Int = size - 1)(implicit comp: Ordering[K]): (Boolean, Int) = {
    if(start > end) return false -> start

    val pos = start + (end - start)/2
    val k = keys(pos)
    val c = comp.compare(key, k)

    if(c == 0) return true -> pos
    if(c < 0) return find(key, start, pos - 1)(comp)

    find(key, pos + 1, end)
  }

  def findPath(key: K)(implicit comp: Ordering[K]): (Block[T, K, V], Int) = {
    val (_, pos) = find(key, 0, size-1)(comp)
    pointers(pos) -> pos
  }

  def setChild(idx: Int, child: Block[T, K, V]): Unit = {
    child.parent = Some(this)
    child.pos = idx
    pointers(idx) = child
  }

  def insert(key: K)(implicit comp: Ordering[K]): Boolean = {
    if(isFull) return false
    insert(find(key)(comp)._2, key)
  }

  protected def insert(idx: Int, key: K): Boolean = {
    for(i<-(size until idx by -1)){
      keys(i) = keys(i-1)
    }

    keys(idx) = key

    size += 1

    true
  }

  /**
    * Insert a key and puts the pointer carrying elements greater than the key at its right side
    */
  def insertRight(key: K, ptr: Block[T, K, V])(implicit comp: Ordering[K]): Boolean = {

    if(isFull) return false

    val (_, idx) = find(key)(comp)

    for(i<-(size until idx by -1)){
      keys(i) = keys(i-1)

      setChild(i+1, pointers(i))
    }

    keys(idx) = key
    setChild(idx + 1, ptr)

    size += 1

    true
  }

  /**
    * Insert a key and puts the pointer carrying elements lesser than the key at its left side
    */
  def insertLeft(key: K, ptr: Block[T, K, V])(implicit comp: Ordering[K]): Boolean = {

    if(isFull) return false

    val (_, idx) = find(key)(comp)

    for(i<-size until idx by -1){
      keys(i) = keys(i-1)

      setChild(i+1, pointers(i))
    }

    setChild(idx + 1, pointers(idx))

    keys(idx) = key
    setChild(idx, ptr)

    size += 1

    true
  }

  def remove(idx: Int): Boolean = {

    size -= 1

    for(i<-idx until size){
      keys(i) = keys(i+1)

      setChild(i+1, pointers(i+2))
    }

    true
  }

  /**
    * Special case for the left rotation or right borrowing
    */
  def removeRight(idx: Int): K = {

    val k = keys(idx)

    size -= 1

    for(i<-idx until size){
      keys(i) = keys(i+1)
      setChild(i, pointers(i+1))
    }

    setChild(size, pointers(size+1))

    k
  }

  def removeAndGet(idx: Int): K = {
    val key = keys(idx)
    remove(idx)
    key
  }

  def left: Option[MetaBlock[T, K, V]] = {
    parent match {
      case None => None
      case Some(parent) =>
        val idx = pos - 1
        if(idx >= 0) Some(parent.pointers(idx).asInstanceOf[MetaBlock[T, K, V]]) else None
    }
  }

  def right: Option[MetaBlock[T, K, V]] = {
    parent match {
      case None => None
      case Some(parent) =>
        val idx = pos + 1
        if(idx <= parent.size) Some(parent.pointers(idx).asInstanceOf[MetaBlock[T, K, V]]) else None

    }
  }

  def inOrder: Seq[K] = keys.slice(0, size)

  override def isFull = size == MAX

  override def toString = s"""[${keys.slice(0, size).mkString(",")}]"""

}
