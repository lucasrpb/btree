package btree

import java.util.UUID

import scala.reflect.ClassTag

class Index[T:ClassTag, K: ClassTag, V: ClassTag](val DATA_ORDER: Int, val META_ORDER: Int)
                                                 (implicit val comp: Ordering[K]) {

  val DATA_MIN = DATA_ORDER - 1
  val DATA_MAX = DATA_ORDER*2 - 1
  val DATA_MIDDLE = DATA_MIN

  val META_MIN = META_ORDER - 1
  val META_MAX = META_ORDER*2 - 1
  val META_MIDDLE = META_MIN

  var root: Option[Block[T, K, V]] = None
  
  protected def find(key: K,
                     start: Option[Block[T, K, V]],
                     parent: Option[MetaBlock[T, K, V]] = None,
                     pos: Int = 0)(comp: Ordering[K]): Option[DataBlock[T, K, V]] = {
    if(start.isEmpty) return None

    start.get match {
      case leaf: DataBlock[T, K, V] =>
        Some(leaf)

      case meta: MetaBlock[T, K, V] =>
        val (start, idx) = meta.findPath(key)(comp)
        find(key, Some(start), Some(meta), idx)(comp)
    }
  }

  def find(key: K)(implicit comp: Ordering[K]): Option[DataBlock[T, K, V]] = {
    find(key, root)(comp)
  }

  protected def splitMetaBlock(k: K, left: MetaBlock[T, K, V], prev: Block[T, K, V]): Boolean = {

    val right = new MetaBlock[T, K, V](UUID.randomUUID.toString.asInstanceOf[T], META_MIN, META_MAX)

    val keys = left.keys
    val pointers = left.pointers
    val up = keys(META_MIDDLE)

    var j = 0

    for(i<-META_MIDDLE+1 until META_MAX){
      right.keys(j) = keys(i)
      right.size += 1

      right.setChild(j, pointers(i))

      left.size -= 1

      j += 1
    }

    // Excludes the middle key!
    left.size -= 1

    right.setChild(j, pointers(META_MAX))

    val c = comp.compare(k, up)

    if(c < 0){
      left.insertRight(k, prev)
    } else {
      right.insertRight(k, prev)
    }

    handleParent(up, left, right)
  }

  protected def handleParent(k: K, left: Block[T, K, V], right: Block[T, K, V]): Boolean = {
    left.parent match {
      case None =>

        val block = new MetaBlock[T, K, V](UUID.randomUUID.toString.asInstanceOf[T], META_MIN, META_MAX)

        block.keys(0) = k
        block.size += 1

        block.setChild(0, left)
        block.setChild(1, right)

        val ROOT = Some(block)

        root = ROOT

        true

      case Some(parent) =>

        if(parent.isFull){
          splitMetaBlock(k, parent, right)
        } else {
          parent.insertRight(k, right)
        }

    }
  }

  protected def splitDataBlock(left: DataBlock[T, K, V], k: K, v: V): Boolean = {

    val right = new DataBlock[T, K, V](UUID.randomUUID.toString.asInstanceOf[T], DATA_MIN, DATA_MAX)
    val keys = left.keys
    val (middle, _) = keys(DATA_MIDDLE)

    val c = comp.compare(k, middle)

    val pos = if(c < 0) DATA_MIDDLE else DATA_MIDDLE + 1

    var j = 0

    for(i<-pos until DATA_MAX){
      right.keys(j) = keys(i)
      right.size += 1
      left.size -= 1
      j += 1
    }

    if(c < 0) left.insert(k -> v) else right.insert(k -> v)

    // Pointers handling...
    right.next = left.next
    right.prev = Some(left)
    left.next = Some(right)

    if(left.next.isDefined){
      left.next.get.prev = Some(right)
    }

    handleParent(keys(left.size - 1)._1, left, right)
  }

  protected def checkFull(leaf: DataBlock[T, K, V], k: K, v: V): Boolean = {
    if(leaf.isFull()){

     /* if(leaf.parent.isDefined && leaf.parent.get.isFull){
        return false
      }*/

      return splitDataBlock(leaf, k, v)
    }

    leaf.insert(k -> v)
  }

  def insert(k: K, v: V)(implicit comp: Ordering[K]): Boolean = {
    find(k)(comp) match {
      case None =>

        val leaf = new DataBlock[T, K, V](UUID.randomUUID.toString.asInstanceOf[T], DATA_MIN, DATA_MAX)
        leaf.insert(0, k -> v)
        root = Some(leaf)

        true

      case Some(leaf) =>

        val (found, _) = leaf.find(k)

        if(found) {
          false
        } else {
          checkFull(leaf, k, v)
        }

    }
  }

  protected def merge(left: DataBlock[T, K, V], right: DataBlock[T, K, V]): DataBlock[T, K, V] = {
    var j = left.size
    val size = right.size

    for(i<-0 until size){
      left.keys(j) = right.keys(i)
      left.size += 1
      j += 1
    }

    // Fixing some pointers...
    val next = right.next

    left.next = next

    if(next.isDefined){
      next.get.prev = Some(left)
    }

    left
  }

  protected def merge(left: MetaBlock[T, K, V], right: MetaBlock[T, K, V], key: K): MetaBlock[T, K, V] = {
    left.insert(key)

    var j = left.size
    val size = right.size
    val keys = right.keys
    val pointers = right.pointers

    for(i<-0 until size){
      left.keys(j) = keys(i)
      left.size += 1

      left.setChild(j, pointers(i))

      j += 1
    }

    left.setChild(j, pointers(size))

    left
  }

  protected def merge(target: MetaBlock[T, K, V], pos: Int, left: Option[MetaBlock[T, K, V]],
                      right: Option[MetaBlock[T, K, V]]): Boolean = {

    val parent = target.parent.get

    target.remove(pos)

    val merged = left match {
      case Some(left) =>
        merge(left, target, parent.keys(left.pos))
      case _ => merge(target, right.get, parent.keys(target.pos))
    }

    // Parent is root :)
    if(parent.parent.isEmpty){

      parent.remove(merged.pos)

      if(parent.isEmpty){
        merged.parent = None
        root = Some(merged)
      }

      return true
    }

    if(parent.hasEnoughKeys){
      return parent.remove(merged.pos)
    }

    borrowFromLeft(parent, merged.pos)
  }

  protected def borrowFromRight(target: MetaBlock[T, K, V], pos: Int,
                                left: Option[MetaBlock[T, K, V]]): Boolean = {
    target.right match {
      case None => merge(target, pos, left, None)
      case Some(right) =>

        val parent = target.parent.get

        if(right.hasEnoughKeys()){

          // Get the subtree of sibling's first key
          val link = right.pointers(0)

          /*
           * The first key from sibling goes up to the parent's separating key position similarly to
           * the left borrowing process
           */
          val up = right.removeRight(0)

          // Parent's separating key goes down
          val down = parent.keys(target.pos)

          // New separating key on the parent
          parent.keys(target.pos) = up

          // Usual removal
          target.remove(pos)

          // Subtree inserted on the right of descending key.
          target.insertRight(down, link)

          true

        } else {
          merge(target, pos, left, Some(right))
        }

    }
  }

  protected def borrowFromLeft(target: MetaBlock[T, K, V], pos: Int): Boolean = {
    target.left match {
      case None => borrowFromRight(target, pos, None)
      case Some(left) =>

        val parent = target.parent.get

        if(left.hasEnoughKeys()){

          // Get the subtree of sibling's last key
          val link = left.pointers(left.size)

          /*
           * On meta blocks we cannot duplicate keys. Parent's separating key must come down to the node where
           * removing was performed and borrowing key must come up to the parent as the new separating key.
          */
          val up = left.removeAndGet(left.size - 1)

          val down = parent.keys(left.pos)

          parent.keys(left.pos) = up

          // Usual removal
          target.remove(pos)

          /*
           * The last subtree of the left sibling contains elements lower than the parent's separating key
           * that came down. So it must be inserted on the left of that key.
           */
          target.insertLeft(down, link)

          true
        } else {
          borrowFromRight(target, pos, Some(left))
        }

    }
  }

  protected def merge(target: DataBlock[T, K, V], pos: Int, left: Option[DataBlock[T, K, V]],
                      right: Option[DataBlock[T, K, V]]): Boolean = {

    println(s"\nMERGING...\n")

    val parent = target.parent.get

    // Usual deletion
    target.remove(pos)

    // Try to merge with leftmost node
    val merged = left match {
      case Some(left) =>
        merge(left, target)

      case None =>
        merge(target, right.get)
    }

    // Parent is root
    if(parent.parent.isEmpty){
      parent.remove(merged.pos)

      // Merged leaf is now the root...
      if(parent.isEmpty()){
        merged.parent = None
        root = Some(merged)
      }

      return true
    }

    // Parent has enough keys
    if(parent.hasEnoughKeys()){
      return parent.remove(merged.pos)
    }

    // Parent doesn't have enough keys! We need to try to borrow. Here we go again...
    borrowFromLeft(parent, merged.pos)
  }

  protected def borrowFromRight(target: DataBlock[T, K, V], pos: Int,
                                left: Option[DataBlock[T, K, V]]): Boolean = {
    target.right match {

      // No right sibling either! We need to perform a merging operation
      case None => merge(target, pos, left, None)
      case Some(right) =>

        val parent = right.parent.get

        if(right.hasEnoughKeys()){

          println(s"\nBORROWING FROM RIGHT DATA BLOCK...\n")

          target.remove(pos)

          // Borrow the first key from right sibling
          target.insert(right.removeAndGet(0))

          // Duplicate the new last key in the parent
          parent.keys(target.pos) = target.keys(target.size - 1)._1

          true

        } else {

          //No enough keys to borrow. We need to merge
          merge(target, pos, left, Some(right))
        }

    }
  }

  protected def borrowFromLeft(target: DataBlock[T, K, V], pos: Int): Boolean = {
    target.left match {
      // No left sibling. Try to borrow from right one...
      case None => borrowFromRight(target, pos, None)
      case Some(left) =>

        val parent = left.parent.get

        // Has the sibling block enough keys so it can borrow one?
        if(left.hasEnoughKeys()){

          println(s"\nBORROWING FROM LEFT DATA BLOCK...\n")

          // Perform the usual deletion operation...
          target.remove(pos)

          // Borrow the last key from the sibling
          target.insert(left.removeAndGet(left.size - 1))

          // On data blocks borrowing we duplicate the new last key from the sibling
          parent.keys(left.pos) = left.keys(left.size - 1)._1

          true

        } else {

          // No enough keys to borrow. Try to borrow from the right sibling (if any)
          borrowFromRight(target, pos, Some(left))
        }

    }
  }

  protected def checkEnoughKeys(target: DataBlock[T, K, V], pos: Int): Boolean = {

    // Is the leaf also the root?
    if(target.parent.isEmpty){

      // We can safely remove from the root even it not having enough keys!
      target.remove(pos)

      // The root is empty. No more data => empty tree!
      if(target.isEmpty){
        root = None
      }

      return true
    }

    // Block is a data block with sufficient keys...
    if(target.hasEnoughKeys()){
      return target.remove(pos)
    }

    // No enough keys. Try to borrow from left sibling...
    borrowFromLeft(target, pos)
  }

  def remove(k: K)(implicit comp: Ordering[K]): Boolean = {

    find(k)(comp) match {
      case None => false
      case Some(leaf) =>

        val (found, pos) = leaf.find(k)

        if(found){
          checkEnoughKeys(leaf, pos)
        } else {
          // Key not present in the tree
          false
        }

    }
  }

  protected def inOrder(start: Block[T, K, V], list: scala.collection.mutable.ArrayBuffer[K]): Unit = {
    start match {
      case data: DataBlock[T, K, V] =>

       // println(s"\nblock: ${data}\n")

        list ++= data.inOrder

      case meta: MetaBlock[T, K, V] =>

        val size = meta.size
        val pointers = meta.pointers

        //println(s"\nmeta: ${meta}\n")

        for(i<-0 to size){
          inOrder(pointers(i), list)
        }

    }
  }

  def inOrder(): scala.collection.mutable.ArrayBuffer[K] = {
    val list = scala.collection.mutable.ArrayBuffer[K]()

    /*root match {
      case None => list
      case Some(root) =>
        inOrder(root, list)
        list
    }*/

    var aux: Option[Block[T, K, V]] = root
    var stop = false

    while(!stop && aux.isDefined){
      aux.get match {
        case block: MetaBlock[T, K, V] => aux = Some(block.pointers(0))
        case block: DataBlock[T, K, V] =>
          aux = Some(block)
          stop = true
      }
    }

    var start: Option[DataBlock[T, K, V]] = aux.asInstanceOf[Option[DataBlock[T, K, V]]]

    while(start.isDefined){
      list ++= start.get.inOrder

      val next = start.get.next
      start = next
    }

    list
  }

  def prettyPrint(): Unit = {

    val levels = scala.collection.mutable.Map[Int, scala.collection.mutable.ArrayBuffer[Block[T, K, V]]]()

    def inOrder(start: Block[T, K, V], level: Int): Unit = {

      val opt = levels.get(level)
      var l: scala.collection.mutable.ArrayBuffer[Block[T, K, V]] = null

      if(opt.isEmpty){
        l = scala.collection.mutable.ArrayBuffer[Block[T, K, V]]()
        levels  += level -> l
      } else {
        l = opt.get
      }

      start match {
        case data: DataBlock[T, K, V] =>
          l += data

        case meta: MetaBlock[T, K, V] =>

          l += meta

          val size = meta.size
          val pointers = meta.pointers

          for(i<-0 to size){
            inOrder(pointers(i), level + 1)
          }

      }
    }

    root match {
      case Some(root) => inOrder(root, 0)
      case _ =>
    }

    levels.keys.toSeq.sorted.foreach { case level =>
        println(s"level[$level]: ${levels(level)}")
    }

    println()

  }

}
