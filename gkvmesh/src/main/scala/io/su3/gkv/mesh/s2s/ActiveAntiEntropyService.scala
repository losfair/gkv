package io.su3.gkv.mesh.s2s

import io.su3.gkv.mesh.storage.DistributedLock
import io.su3.gkv.mesh.background.UniqueBackgroundService
import io.su3.gkv.mesh.storage.MerkleTreeIncrementalBuilder
import java.security.SecureRandom
import io.grpc.ManagedChannelBuilder
import com.typesafe.scalalogging.Logger
import io.su3.gkv.mesh.proto.s2s.MeshGrpc
import java.util.concurrent.Semaphore
import io.su3.gkv.mesh.util.WorkerGroup
import io.su3.gkv.mesh.proto.s2s.MeshGrpc.MeshBlockingStub
import io.su3.gkv.mesh.proto.s2s.PullMerkleTreeRequest
import com.google.protobuf.ByteString
import io.su3.gkv.mesh.util.MerkleTreeUtil
import io.su3.gkv.mesh.storage.TkvKeyspace
import io.su3.gkv.mesh.proto.persistence.MerkleNode
import org.apache.commons.codec.binary.Hex
import io.su3.gkv.mesh.proto.persistence.MerkleChild
import io.su3.gkv.mesh.proto.s2s.PullLeafRequest
import io.su3.gkv.mesh.util.UniqueVersionUtil
import io.su3.gkv.mesh.storage.MerkleTreeTxn
import io.su3.gkv.mesh.util.BytesUtil
import io.su3.gkv.mesh.proto.persistence.MerkleLeaf
import scala.collection.mutable.ArrayBuffer
import io.su3.gkv.mesh.storage.ClusterMetadata
import io.su3.gkv.mesh.proto.persistence.PeerInfo
import io.su3.gkv.mesh.config.MeshServiceConfig
import io.su3.gkv.mesh.gclock.GClock

object ActiveAntiEntropyService extends UniqueBackgroundService {
  private val rng = SecureRandom()
  private val logger = Logger(getClass())

  override def serviceName: String = "ActiveAntiEntropyService"
  override def runForever(lock: DistributedLock): Unit = {
    val ourClusterId = ClusterMetadata.getClusterId(lock.tkv)

    while (true) {
      val upstreams = MeshMetadata.get.snapshot.peers
        .getOrElse(ourClusterId, PeerInfo())
        .upstreams
      runOnce(lock, upstreams)
      val jitter = rng.nextLong(5000)
      Thread.sleep(20000 + jitter)
    }
  }

  def runOnce(lock: DistributedLock, upstreams: Iterable[String]): Unit = {
    MerkleTreeIncrementalBuilder.runIncrementalBuild(lock)
    val wg = WorkerGroup("aae-pull", 4)
    try {
      upstreams.foreach { x => wg.spawnOrBlock { pullFromPeer(lock, x) } }
      wg.waitUntilIdle()
      if (!upstreams.isEmpty) {
        logger.info("Finished pulling from {} upstreams", upstreams.size)
      }
    } finally {
      val interrupted = wg.close()
      if (interrupted != 0) {
        logger.warn("Interrupted {} upstream pull tasks", interrupted)
      }
    }
  }

  private def pullFromPeer(lock: DistributedLock, peerAddress: String): Unit = {
    val channel = ManagedChannelBuilder
      .forTarget(peerAddress)
      .usePlaintext()
      .defaultServiceConfig(MeshServiceConfig.serviceConfig)
      .enableRetry()
      .build()
    val stub = MeshGrpc.blockingStub(channel)
    val startTime = System.currentTimeMillis()

    val wg = WorkerGroup("aae-pull-" + peerAddress, 256)
    try {
      PeerPuller(lock, wg, stub).pullOnce(Array.emptyByteArray, None)
      wg.waitUntilIdle()
      val endTime = System.currentTimeMillis()
      logger.info("Pull from {} done in {}ms", peerAddress, endTime - startTime)
    } finally {
      val interrupted = wg.close()
      if (interrupted != 0) {
        logger.warn("Interrupted {} peer pull subtasks", interrupted)
      }
      channel.shutdownNow()
      channel.awaitTermination(1, java.util.concurrent.TimeUnit.SECONDS)
    }
  }
}

private class PeerPuller(
    lock: DistributedLock,
    wg: WorkerGroup,
    stub: MeshBlockingStub
) {
  final def pullOnce(
      prefix: Array[Byte],
      knownNode: Option[MerkleNode]
  ): Unit = {
    if (knownNode.isDefined) {
      PeerPuller.logger.debug(
        "Using known prefix '{}'",
        Hex.encodeHexString(prefix)
      )
    } else {
      PeerPuller.logger.debug(
        "Pulling prefix '{}'",
        Hex.encodeHexString(prefix)
      )
    }

    val localNode = lock.tkv.transact { txn =>
      lock.validate(txn)
      txn
        .get(TkvKeyspace.constructMerkleTreeStructureKey(prefix.toSeq))
        .map(MerkleNode.parseFrom(_))
        .getOrElse(MerkleNode())
    }
    val localHash = MerkleTreeUtil.hashNode(localNode)
    val localChildren = localNode.children.map { x => (x.index, x) }.toMap

    val remoteChildren = (knownNode match {
      case Some(n) =>
        if (MerkleTreeUtil.hashNode(n) == localHash) {
          return
        }

        n.children
      case None =>
        val res = stub.pullMerkleTree(
          PullMerkleTreeRequest(
            prefix = ByteString.copyFrom(prefix),
            hash = ByteString.copyFrom(localHash)
          )
        )

        if (res.identical) {
          return
        }

        res.children
    }).map { x => (x.index, x) }.toMap

    val diff = remoteChildren.flatMap { case (index, remoteChild) =>
      localChildren.get(index) match {
        case Some(localChild) =>
          if (localChild.hash == remoteChild.hash) {
            None
          } else {
            Some(remoteChild)
          }
        case None => Some(remoteChild)
      }
    }

    if (prefix.length == 31) {
      // children are leaves
      pullLeafOnce(prefix, diff)
    } else if (diff.size == 1) {
      // Collapse
      var child = diff.head
      val newPrefix = ArrayBuffer[Byte]()
      newPrefix.sizeHint(32)
      newPrefix ++= prefix
      newPrefix += child.index.toByte

      while (
        child.inlineNode.isDefined && child.inlineNode.get.children.size == 1 && newPrefix.length < 31
      ) {
        child = child.inlineNode.get.children(0)
        newPrefix += child.index.toByte
      }

      // Tail recursion
      pullOnce(newPrefix.toArray, child.inlineNode)
    } else {
      diff.foreach { child =>
        val extendedPrefix = prefix ++ Array(child.index.toByte)
        wg.spawnOrBlock {
          pullOnce(extendedPrefix, child.inlineNode)
        }
      }
    }
  }

  private def pullLeafOnce(
      prefix: Array[Byte],
      children: Iterable[MerkleChild]
  ): Unit = {
    val res =
      stub.pullLeaf(PullLeafRequest(hashes = children.map { c =>
        ByteString.copyFrom(prefix ++ Array(c.index.toByte))
      }.toSeq))

    if (!res.entries.isEmpty) {
      lock.tkv.transact { txn =>
        lock.validate(txn)
        val mt = MerkleTreeTxn(txn)
        res.entries.foreach(mt.mergeLeaf(lock.gclock, _))
      }
    }
  }
}

private object PeerPuller {
  private val logger = Logger(getClass())
}
