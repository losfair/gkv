package io.su3.gkv.mesh.storage

import com.apple.foundationdb.tuple.Tuple;
import com.typesafe.scalalogging.Logger
import java.security.SecureRandom
import io.su3.gkv.mesh.storage.DistributedLock.DistributedLockException
import org.slf4j.MDC
import scala.util.control.NonFatal.apply
import scala.util.control.NonFatal

object DistributedLock {
  val logger = Logger(getClass())
  val rng = SecureRandom()
  val lockTimeoutMs = 10000 // 10 seconds
  val lockRenewIntervalMs = 1000 // 1 second

  private def tryAcquire(
      tkv: Tkv,
      txn: TkvTxn,
      name: String,
      priority: Int
  ): Option[DistributedLock] = {
    val kvKey = Tuple.from(TkvKeyspace.distributedLockPrefix, name).pack()
    val currentTime = System.currentTimeMillis()
    val current =
      txn.get(kvKey)

    // Should we try to acquire the lock?
    current match {
      case Some(value) =>
        val lock = scalapb.json4s.JsonFormat
          .fromJsonString[io.su3.gkv.mesh.proto.persistence.DistributedLock](
            String(value)
          )
        val timeDiff = currentTime - lock.realTimestamp
        if (timeDiff < lockTimeoutMs && lock.priority <= priority) {
          return None // lock held by another process
        }
      case None =>
    }

    // Acquire the lock!
    val token = java.util.UUID.randomUUID().toString()
    val lock = io.su3.gkv.mesh.proto.persistence
      .DistributedLock(
        realTimestamp = currentTime,
        token = token,
        priority = priority
      )
    txn.put(kvKey, scalapb.json4s.JsonFormat.toJsonString(lock).getBytes())
    Some(DistributedLock(tkv, name, token))
  }

  def acquire(tkv: Tkv, name: String, priority: Int = 0): DistributedLock = {
    while (true) {
      try {
        val ret = tkv.transact { txn => tryAcquire(tkv, txn, name, priority) }
        ret match {
          case Some(lock) =>
            lock.startRenewTask()
            return lock
          case None =>
        }
      } catch {
        case NonFatal(e) =>
          logger.error("Failed to acquire distributed lock", e)
      }

      val jitter = rng.nextLong(1000)
      Thread.sleep(1000 + jitter)
    }
    ???
  }

  class DistributedLockException(msg: String) extends IllegalStateException(msg)
}

class DistributedLock private (
    val tkv: Tkv,
    val name: String,
    val token: String
) extends AutoCloseable {
  private var renewTask: Option[Thread] = None
  private val kvKey = Tuple.from(TkvKeyspace.distributedLockPrefix, name).pack()

  private def startRenewTask(): Unit = {
    renewTask = Some(Thread.startVirtualThread(new Runnable {
      override def run(): Unit = {
        Thread.currentThread().setName(s"gkvmesh-dlrenew-$name")

        MDC.put("task", "distributedLockRenew")
        MDC.put("lockName", name)
        MDC.put("lockToken", token)

        while (true) {
          try {
            renewOnce()
            Thread.sleep(DistributedLock.lockRenewIntervalMs)
          } catch {
            case e: DistributedLockException =>
              DistributedLock.logger
                .error("Failed to renew distributed lock: {}", e.getMessage())
              return
            case NonFatal(e) =>
              DistributedLock.logger
                .error("Failed to renew distributed lock", e)
              return
            case _: InterruptedException =>
              DistributedLock.logger.info("Renew task interrupted")
              return
          }
        }
      }
    }))
  }

  override def close(): Unit = {
    renewTask match {
      case Some(x) =>
        x.interrupt()
        x.join()
      case None =>
    }
  }

  def release(txn: TkvTxn): Unit = {
    validate(txn)
    txn.delete(kvKey)
  }

  def validate(txn: TkvTxn): Unit = {
    val current = txn.get(kvKey)

    current match {
      case Some(value) =>
        val lock = scalapb.json4s.JsonFormat
          .fromJsonString[io.su3.gkv.mesh.proto.persistence.DistributedLock](
            String(value)
          )
        if (lock.token != token) {
          throw new DistributedLockException("Lock acquired by another process")
        }
      case None =>
        throw new DistributedLockException("Lock deleted")
    }
  }

  private def renewOnce(): Unit = {
    tkv.transact { txn =>
      validate(txn)
      val currentTime = System.currentTimeMillis()
      val lock = io.su3.gkv.mesh.proto.persistence
        .DistributedLock(realTimestamp = currentTime, token)
      txn.put(kvKey, scalapb.json4s.JsonFormat.toJsonString(lock).getBytes())
    }
  }
}
