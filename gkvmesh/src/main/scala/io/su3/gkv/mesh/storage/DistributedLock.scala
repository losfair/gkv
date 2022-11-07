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
    val kvOwnerKey =
      Tuple.from(TkvKeyspace.distributedLockPrefix, name, "owner").pack()
    val kvTimestampKey =
      Tuple.from(TkvKeyspace.distributedLockPrefix, name, "ts").pack()

    val currentTime = System.currentTimeMillis()
    val currentOwner =
      txn.get(kvOwnerKey)
    val currentTimestamp =
      txn.get(kvTimestampKey)

    // Should we try to acquire the lock?
    (currentOwner, currentTimestamp) match {
      case (Some(value), Some(timestampBytes)) =>
        val lock = scalapb.json4s.JsonFormat
          .fromJsonString[io.su3.gkv.mesh.proto.persistence.DistributedLock](
            String(value)
          )
        val timestamp = String(timestampBytes).toLong
        val timeDiff = currentTime - timestamp
        if (timeDiff < lockTimeoutMs && lock.priority <= priority) {
          return None // lock held by another process
        }
      case _ =>
    }

    // Acquire the lock!
    val token = java.util.UUID.randomUUID().toString()
    val lock = io.su3.gkv.mesh.proto.persistence
      .DistributedLock(
        token = token,
        priority = priority
      )
    txn.put(kvOwnerKey, scalapb.json4s.JsonFormat.toJsonString(lock).getBytes())
    txn.put(
      kvTimestampKey,
      currentTime.toString.getBytes()
    )
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

  private val kvOwnerKey =
    Tuple.from(TkvKeyspace.distributedLockPrefix, name, "owner").pack()
  private val kvTimestampKey =
    Tuple.from(TkvKeyspace.distributedLockPrefix, name, "ts").pack()

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
    txn.delete(kvOwnerKey)
    txn.delete(kvTimestampKey)
  }

  def validate(txn: TkvTxn): Unit = {
    val current = txn.get(kvOwnerKey)

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
      txn.put(kvTimestampKey, currentTime.toString.getBytes())
    }
  }
}
