package io.su3.gkv.mesh.util

import java.util.concurrent.Semaphore
import com.typesafe.scalalogging.Logger
import scala.collection.mutable
import java.util.concurrent.atomic.AtomicLong
import scala.util.control.NonFatal
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.atomic.AtomicInteger

class WorkerGroup(name: String, concurrency: Int) {
  val lock = ReentrantLock()
  val logger = Logger(getClass())
  val sem = Semaphore(concurrency)
  val activeSet = mutable.Set[Thread]()
  private val interruptCount = AtomicInteger(0)
  private var closed = false

  private def spawnInternal(f: => Unit, canRunSynchronously: Boolean): Unit = {
    if (canRunSynchronously) {
      if (!sem.tryAcquire()) {
        return f
      }
    } else {
      sem.acquire()
    }

    lock.lock()
    try {
      if (closed) {
        sem.release()
        throw new IllegalStateException("WorkerGroup is closed")
      }
      var th: Thread = null
      th = Thread
        .ofVirtual()
        .name(s"wg-$name")
        .unstarted(() => {
          try {
            f
          } catch {
            case NonFatal(e) =>
              logger.error("uncaught exception", e)
            case e: InterruptedException =>
              interruptCount.incrementAndGet()
          } finally {
            lock.lock()
            activeSet.remove(th)
            lock.unlock()
            sem.release()
          }
        })
      activeSet += th
      th.start()
    } finally {
      lock.unlock()
    }

  }

  def spawn(f: => Unit): Unit = {
    spawnInternal(f, false)
  }

  def spawnOrBlock(f: => Unit): Unit = {
    spawnInternal(f, true)
  }

  def waitUntilIdle(): Unit = {
    sem.acquire(concurrency)
    sem.release(concurrency)
  }

  def close(): Int = {
    lock.lock()
    val lastActiveSet =
      try {
        if (closed) {
          return 0
        }

        closed = true
        activeSet.toSeq
      } finally {
        lock.unlock()
      }

    lastActiveSet.foreach(_.interrupt())
    lastActiveSet.foreach(_.join())

    interruptCount.get()
  }
}
