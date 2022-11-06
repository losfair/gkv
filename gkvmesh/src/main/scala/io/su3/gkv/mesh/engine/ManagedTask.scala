package io.su3.gkv.mesh.engine

import com.typesafe.scalalogging.Logger

abstract class ManagedTask {
  private var worker: Option[Thread] = None
  protected val logger = Logger(getClass())

  def start(): Unit = {
    worker = Some(Thread.startVirtualThread(() => {
      try {
        run()
      } catch {
        case _: InterruptedException =>
      }
    }))
  }

  def close(): Unit = {
    worker.foreach(_.interrupt())
    worker.foreach(_.join())
  }

  protected def run(): Unit
}
