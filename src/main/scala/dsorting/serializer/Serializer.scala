package dsorting.serializer

trait Serializer[T] {
  def toByteArray(target: T): Array[Byte]
  def fromByteArray(array: Array[Byte]): T
}
