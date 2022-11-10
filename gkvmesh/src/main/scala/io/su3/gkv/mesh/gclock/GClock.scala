package io.su3.gkv.mesh.gclock

trait GClock {
  def isReady(): Boolean
  def now(): Long
  def after(t: Long): Boolean
  def before(t: Long): Boolean

  def waitUntilReady(): Unit = {
    while (!isReady()) {
      Thread.sleep(100)
    }
  }
}
