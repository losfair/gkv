package io.su3.gkv.mesh.storage

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

class MeshMetadata(tkv: Tkv) {
  private val rng = SecureRandom()
  private val logger = Logger(getClass())
  private var worker: Option[Thread] = None

  case class Snapshot(
      peers: Map[String, PeerInfo] // clusterId -> PeerInfo
  )

  val peersPrefix = "@system/peers/".getBytes()

  private val currentSnapshot = AtomicReference[Snapshot](Snapshot(Map()))

  def snapshot = currentSnapshot.get()

  def start(): Unit = {
    val initialWait = CountDownLatch(1)

    val th = Thread.startVirtualThread(() => {
      while (true) {
        try {
          val newSS = pullOnce()
          val oldSS = currentSnapshot.get()

          for ((clusterId, peerInfo) <- newSS.peers) {
            if (!oldSS.peers.contains(clusterId)) {
              logger.info(s"New peer: $clusterId")
            }
          }

          for ((clusterId, peerInfo) <- oldSS.peers) {
            if (!newSS.peers.contains(clusterId)) {
              logger.info(s"Removed peer: $clusterId")
            }
          }

          for ((clusterId, peerInfo) <- newSS.peers) {
            oldSS.peers.get(clusterId) match {
              case Some(oldPeerInfo) =>
                if (peerInfo.address != oldPeerInfo.address) {
                  logger.info(s"Peer address changed: $clusterId")
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
    })
    worker = Some(th)

    initialWait.await()
    MeshMetadata.singleton = Some(this)
  }

  def close(): Unit = {
    worker.foreach(_.interrupt())
    worker.foreach(_.join())
  }

  private def pullOnce(): Snapshot = {
    val ourClusterId = ClusterMetadata.getClusterId(tkv)

    tkv.transact { txn =>
      val mt = MerkleTreeTxn(txn)
      val peers = mt
        .range(peersPrefix, peersPrefix ++ Array(0xff.toByte), 1000)
        .flatMap { case (k, v) =>
          val clusterId = String(k.drop(peersPrefix.length))
          if (clusterId == ourClusterId) {
            None
          } else {
            Try(
              scalapb.json4s.JsonFormat
                .fromJsonString[PeerInfo](String(v))
            ) match {
              case Success(peerInfo) =>
                Some(clusterId -> peerInfo)
              case Failure(e) =>
                logger.warn("Failed to parse peer info for {}", clusterId, e)
                None
            }
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
