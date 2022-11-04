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

object ActiveAntiEntropyService extends UniqueBackgroundService {
  private val rng = SecureRandom()
  private val logger = Logger(getClass())

  override def serviceName: String = "ActiveAntiEntropyService"
  override def runForever(lock: DistributedLock): Unit = {
    while (true) {
      runOnce(lock, Seq())
      val jitter = rng.nextLong(1000)
      Thread.sleep(5000 + jitter)
    }
  }

  def runOnce(lock: DistributedLock, peers: Iterable[String]): Unit = {
    MerkleTreeIncrementalBuilder.runIncrementalBuild(lock)
    val wg = WorkerGroup("aae-pull", 4)
    try {
      peers.foreach { x => wg.spawnOrBlock { pullFromPeer(lock, x) } }
      wg.waitUntilIdle()
      if (!peers.isEmpty) {
        logger.info("Finished pulling from {} peers", peers.size)
      }
    } finally {
      val interrupted = wg.close()
      if (interrupted != 0) {
        logger.warn("Interrupted {} peer pull tasks", interrupted)
      }
    }
  }

  private def pullFromPeer(lock: DistributedLock, peerAddress: String): Unit = {
    val channel = ManagedChannelBuilder
      .forTarget(peerAddress)
      .usePlaintext()
      .build()
    val stub = MeshGrpc.blockingStub(channel)

    val wg = WorkerGroup("aae-pull-" + peerAddress, 16)
    try {
      PeerPuller(lock, wg, stub).pullOnce(Array.emptyByteArray, None)
      wg.waitUntilIdle()
      logger.info("Pull from {} done", peerAddress)
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
  def pullOnce(prefix: Array[Byte], knownNode: Option[MerkleNode]): Unit = {
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

    lock.tkv.transact { txn =>
      lock.validate(txn)

      res.entries.foreach { leaf =>
        val dataKeyHash = MerkleTreeTxn.hashDataKey(leaf.key.toByteArray)
        val incomingVersion = UniqueVersionUtil
          .serializeUniqueVersion(leaf.version.get)

        // First, check whether we have written to this key locally
        // Then, check the Merkle tree
        val currentVersion =
          txn
            .get(MerkleTreeTxn.rawMerkleTreeHashBufferPrefix ++ dataKeyHash)
            .map(MerkleLeaf.parseFrom(_))
            .orElse(
              txn
                .get(
                  TkvKeyspace.constructMerkleTreeStructureKey(dataKeyHash.toSeq)
                )
                .flatMap(MerkleNode.parseFrom(_).leaf)
            )
            .flatMap(_.version)
            .map(UniqueVersionUtil.serializeUniqueVersion(_))
            .getOrElse(Array.emptyByteArray)

        if (BytesUtil.compare(incomingVersion, currentVersion) > 0) {
          val mt = MerkleTreeTxn(txn)
          if (leaf.deleted) {
            mt.delete(leaf.key.toByteArray, Some(leaf.version.get))
          } else {
            mt.put(
              leaf.key.toByteArray,
              leaf.value.toByteArray,
              Some(leaf.version.get)
            )
          }
        }
      }
    }

  }
}

private object PeerPuller {
  private val logger = Logger(getClass())
}
