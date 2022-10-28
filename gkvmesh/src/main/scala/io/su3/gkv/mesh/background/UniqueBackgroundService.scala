package io.su3.gkv.mesh.background

import io.su3.gkv.mesh.storage.DistributedLock
import io.su3.gkv.mesh.storage.Tkv
import com.typesafe.scalalogging.Logger
import scala.util.control.NonFatal

trait UniqueBackgroundService {
  def serviceName: String
  def runForever(lock: DistributedLock): Unit
}

object UniqueBackgroundService {
  val logger = Logger(getClass())

  def run(tkv: Tkv, service: UniqueBackgroundService): Unit = {
    val serviceName = service.serviceName

    while (true) {
      val lock = DistributedLock.acquire(tkv, serviceName)
      logger.info(
        "Acquired lock for service {}, token: {}",
        serviceName,
        lock.token
      )
      try {
        service.runForever(lock)
        throw new RuntimeException("runForever() returned")
      } catch {
        case NonFatal(e) =>
          logger.error("Service {} failed", serviceName, e)
      } finally {
        lock.close()
      }

      Thread.sleep(1000)
    }
  }

  def spawn(tkv: Tkv, service: UniqueBackgroundService): Thread = {
    Thread.startVirtualThread(new Runnable {
      override def run(): Unit = {
        Thread.currentThread().setName(s"gkvmesh-unique-${service.serviceName}")

        UniqueBackgroundService.run(tkv, service)
      }
    })
  }
}
