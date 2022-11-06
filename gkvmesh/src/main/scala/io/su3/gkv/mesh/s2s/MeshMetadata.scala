package io.su3.gkv.mesh.s2s

import io.su3.gkv.mesh.proto.persistence.PeerInfo
import java.util.concurrent.atomic.AtomicReference
import java.security.SecureRandom
import scala.util.control.NonFatal.apply
import scala.util.control.NonFatal
import com.typesafe.scalalogging.Logger
import java.util.concurrent.CountDownLatch
import scala.util.Try
import scala.util.Success
import scala.util.Failure
import io.su3.gkv.mesh.storage.Tkv
import io.su3.gkv.mesh.storage.MerkleTreeTxn
import java.util.concurrent.ConcurrentHashMap
import io.su3.gkv.mesh.proto.s2s.Leaf

class MeshMetadata(tkv: Tkv) {
  private val rng = SecureRandom()
  private val logger = Logger(getClass())
  private var worker: Option[Thread] = None

  case class Snapshot(
      peers: Map[String, PeerInfo] // clusterId -> PeerInfo
  )

  val peersPrefix = "@system/peers/".getBytes()

  private val currentSnapshot = AtomicReference[Snapshot](Snapshot(Map()))
  private val peerPushers = ConcurrentHashMap[String, PeerPusher]()

  def snapshot = currentSnapshot.get()

  def start(): Unit = {
    val initialWait = CountDownLatch(1)

    val th = Thread.startVirtualThread(() => {
      try {
        run(initialWait)
      } catch {
        case _: InterruptedException =>
      }
    })
    worker = Some(th)

    initialWait.await()
    MeshMetadata.singleton = Some(this)
  }

  def close(): Unit = {
    worker.foreach(_.interrupt())
    worker.foreach(_.join())

    // Wait for peer pusher inbox drain
    var drained = false
    while (!drained) {
      var nextDrained = true
      peerPushers.values().forEach { x =>
        if (!x.inbox.isEmpty()) {
          nextDrained = false
        }
      }
      drained = nextDrained
      Thread.sleep(100)
    }

    logger.info("Peer pusher inboxes are drained")

    // The last `sleep` should hopefully be enough to let all activity propagate to
    // `wg.spawn`... Any race here is left to AAE to resolve.
    peerPushers.values().forEach { x =>
      x.wg.waitUntilIdle()
      x.wg.close()
    }

    peerPushers.values().forEach(_.close())
  }

  def broadcastNewLeaf(leaf: Leaf): Unit = {
    val ss = snapshot

    for ((clusterId, _) <- ss.peers) {
      val pusher = peerPushers.get(clusterId)
      if (pusher != null) {
        pusher.inbox.offer(leaf)
      }
    }
  }

  private def run(initialWait: CountDownLatch): Unit = {
    while (true) {
      try {
        val newSS = pullOnce()
        val oldSS = currentSnapshot.get()

        for ((clusterId, peerInfo) <- newSS.peers) {
          if (!oldSS.peers.contains(clusterId)) {
            logger.info(s"New peer: $clusterId")
            val pusher = new PeerPusher(tkv, clusterId, peerInfo)
            pusher.start()
            peerPushers.put(clusterId, pusher)
          }
        }

        for ((clusterId, peerInfo) <- oldSS.peers) {
          if (!newSS.peers.contains(clusterId)) {
            logger.info(s"Removed peer: $clusterId")
            val pusher = peerPushers.remove(clusterId)
            if (pusher != null) {
              pusher.close()
            }
          }
        }

        for ((clusterId, peerInfo) <- newSS.peers) {
          oldSS.peers.get(clusterId) match {
            case Some(oldPeerInfo) =>
              if (peerInfo.address != oldPeerInfo.address) {
                logger.info(s"Peer address changed: $clusterId")
                val pusher = peerPushers.remove(clusterId)
                if (pusher != null) {
                  pusher.close()
                }
                val newPusher = new PeerPusher(tkv, clusterId, peerInfo)
                newPusher.start()
                peerPushers.put(clusterId, newPusher)
              }
            case None =>
          }
        }

        currentSnapshot.lazySet(newSS)
      } catch {
        case NonFatal(e) => {
          logger.error("Failed to pull metadata", e)
        }
      }
      initialWait.countDown()
      val jitter = rng.nextLong(1000)
      Thread.sleep(5000 + jitter)
    }
  }

  private def pullOnce(): Snapshot = {
    tkv.transact { txn =>
      val mt = MerkleTreeTxn(txn)
      val peers = mt
        .range(peersPrefix, peersPrefix ++ Array(0xff.toByte), 1000)
        .flatMap { case (k, v) =>
          val clusterId = String(k.drop(peersPrefix.length))
          Try(
            scalapb.json4s.JsonFormat
              .fromJsonString[PeerInfo](String(v))
          ) match {
            case Success(peerInfo) =>
              Some(clusterId -> peerInfo)
            case Failure(e) =>
              logger.warn(
                "Failed to parse peer info for cluster '{}'",
                clusterId,
                e
              )
              None
          }
        }
        .toMap
      Snapshot(peers = peers)
    }
  }
}

object MeshMetadata {
  private var singleton: Option[MeshMetadata] = None

  def get: MeshMetadata = {
    singleton.getOrElse(
      throw new IllegalStateException("MeshMetadata task not started")
    )
  }
}

class Msh
