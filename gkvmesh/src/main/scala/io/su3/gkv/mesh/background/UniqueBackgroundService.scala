package io.su3.gkv.mesh.background

import io.su3.gkv.mesh.storage.DistributedLock
import io.su3.gkv.mesh.storage.Tkv
import com.typesafe.scalalogging.Logger
import scala.util.control.NonFatal
import io.su3.gkv.mesh.storage.DistributedLock.DistributedLockException

trait UniqueBackgroundService {
  def serviceName: String
  def runForever(lock: DistributedLock): Unit
}

object UniqueBackgroundService {
  val logger = Logger(getClass())

  def takeover[T](
      tkv: Tkv,
      service: UniqueBackgroundService,
      priority: Int
  )(f: DistributedLock => T): T = {
    val lock = DistributedLock.acquire(tkv, service.serviceName, priority)
    logger.info(
      "Acquired lock, token: {}",
      lock.token
    )

    try {
      f(lock)
    } finally {
      try {
        tkv.transact { txn => lock.release(txn) }
      } catch {
        case e: DistributedLockException =>
          logger.error(
            "Failed to release lock: {}",
            e.getMessage()
          )
        case NonFatal(e) =>
          logger.error(
            "Failed to release lock",
            e
          )
      }

      lock.close()
    }
  }

  def run(tkv: Tkv, service: UniqueBackgroundService): Unit = {
    val serviceName = service.serviceName

    while (true) {
      try {
        takeover(tkv, service, 0) { lock =>
          service.runForever(lock)
          throw new RuntimeException("runForever() returned")
        }
      } catch {
        case e: DistributedLockException =>
          logger.error("Lock failure: {}", e.getMessage())
        case NonFatal(e) =>
          logger.error("Service failed", e)
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
