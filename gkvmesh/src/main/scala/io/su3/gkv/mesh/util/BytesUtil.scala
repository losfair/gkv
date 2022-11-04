package io.su3.gkv.mesh.util

object BytesUtil {
  def compare(x: Iterable[Byte], y: Iterable[Byte]): Int = {
    val xe = x.iterator
    val ye = y.iterator

    while (xe.hasNext && ye.hasNext) {
      val left = xe.next().toInt & 0xff
      val right = ye.next().toInt & 0xff
      val res = left.compare(right)
      if (res != 0) return res
    }

    xe.hasNext.compare(ye.hasNext)
  }

  class UnsignedBytesOrdering[T[Byte] <: Iterable[Byte]] extends Ordering[T[Byte]] {
    override def compare(x: T[Byte], y: T[Byte]): Int =
      BytesUtil.compare(x, y)
  }
}
