package btree

trait Block[T, K, V] {

  val id: T

  val MIN: Int
  val MAX: Int

  var parent: Option[MetaBlock[T, K, V]] = None

  var pos: Int = 0
  var size: Int = 0

  def isFull() = size == MAX
  def hasEnoughKeys() = size > MIN
  def hasMinimumKeys() = size >= MIN
  def isEmpty() = size == 0

}
