package io.su3.gkv.mesh.storage

import io.su3.gkv.mesh.proto.persistence.MerkleNode
import scala.collection.mutable.HashMap
import java.util.concurrent.CompletableFuture
import io.su3.gkv.mesh.proto.persistence.MerkleLeaf
import scala.collection.mutable.ArrayBuffer
import io.su3.gkv.mesh.proto.persistence.MerkleChild
import io.su3.gkv.mesh.util.MerkleTreeUtil
import com.google.protobuf.ByteString
import com.typesafe.scalalogging.Logger
import org.apache.commons.codec.binary.Hex
import io.su3.gkv.mesh.util.UniqueVersionUtil
import scala.math.Ordering.Implicits._

object MerkleTreeIncrementalBuilder {
  private val lastHash = Array.fill(32)(0xff.toByte)
  private val logger = Logger(getClass())

  private case class BuildResult(numKeys: Int, nextCursor: Option[Array[Byte]])

  def runIncrementalBuild(lock: DistributedLock): Unit = {
    var hashCursor: Option[Array[Byte]] = Some(Array.fill(32)(0.toByte))

    while (true) {
      hashCursor match {
        case Some(x) =>
          var newRoot: Option[Array[Byte]] = None
          val result = lock.tkv.transact { txn =>
            lock.validate(txn)
            val result = runIncrementalBuildOnce(txn, x, 100)
            newRoot = txn.get(
              TkvKeyspace.constructMerkleTreeStructureKey(Seq())
            )
            result
          }
          logger.info(
            "from: {} numKeys: {} newRoot: {}",
            Hex.encodeHexString(x),
            result.numKeys,
            newRoot.map { x => Hex.encodeHexString(x) }.getOrElse("-")
          )
          hashCursor = result.nextCursor
        case None => return
      }
    }
  }

  private def runIncrementalBuildOnce(
      txn: TkvTxn,
      hashCursor: Array[Byte],
      limit: Int
  ): BuildResult = {
    assert(hashCursor.length == 32)

    val range = txn
      .snapshotRange(
        (MerkleTreeTxn.rawMerkleTreeHashBufferPrefix ++ hashCursor)
          .appended(0.toByte),
        MerkleTreeTxn.rawMerkleTreeHashBufferPrefix ++ lastHash,
        limit
      )
    if (range.isEmpty) {
      return BuildResult(numKeys = 0, nextCursor = None)
    }

    val dirtyNodeFutures =
      HashMap[Seq[Byte], CompletableFuture[Option[MerkleNode]]]()
    for (i <- Range.inclusive(32, 0)) {
      for ((k, v) <- range) {
        val hashPrefix =
          k.drop(MerkleTreeTxn.rawMerkleTreeHashBufferPrefix.length)
            .take(i)
            .toSeq
        if (!dirtyNodeFutures.contains(hashPrefix)) {
          val fut =
            txn.asyncGet(
              TkvKeyspace.constructMerkleTreeStructureKey(hashPrefix)
            )
          dirtyNodeFutures.put(
            hashPrefix,
            fut.thenApply { x =>
              x.map { x =>
                MerkleNode.parseFrom(x)
              }
            }
          )
        }
      }
    }

    val dirtyNodes =
      dirtyNodeFutures.iterator.map({ x => (x._1, x._2.join()) }).toMap

    var propagatedUpdates: Map[Seq[Byte], MerkleNode] = range.flatMap {
      case (k, v) =>
        // Write level 32 - leaf
        val hash =
          k.drop(MerkleTreeTxn.rawMerkleTreeHashBufferPrefix.length).toSeq

        val leaf = MerkleLeaf.parseFrom(v)
        val current = dirtyNodes.get(hash).get

        // Only update if the incoming version is newer
        if (
          current.isDefined && UniqueVersionUtil
            .serializeUniqueVersion(leaf.version.get)
            .toSeq <= UniqueVersionUtil
            .serializeUniqueVersion(
              current.get.leaf.get.version.get
            )
            .toSeq
        ) {
          None
        } else {
          val node = MerkleNode(children = Seq(), leaf = Some(leaf))
          Some(
            (
              hash,
              node
            )
          )
        }
    }.toMap

    for (i <- Range.inclusive(32, 0)) {
      val children =
        HashMap[Seq[Byte], HashMap[Int, MerkleChild]]()
      for ((k, v) <- propagatedUpdates) {
        assert(k.length == i)
        txn.put(TkvKeyspace.constructMerkleTreeStructureKey(k), v.toByteArray)

        if (i != 0) {
          val hashPrefix = k.take(i - 1)
          val index = k(i - 1)
          val current = dirtyNodes.get(hashPrefix).get

          val map = children.getOrElseUpdate(
            hashPrefix,
            current
              .map { x => HashMap.from(x.children.map { c => (c.index, c) }) }
              .getOrElse(HashMap())
          )
          val child = MerkleChild(
            index = index.toInt,
            hash = ByteString.copyFrom(MerkleTreeUtil.hashNode(v))
          )
          map.put(child.index, child)
        }
      }
      propagatedUpdates = children.map { case (k, v) =>
        (k, MerkleNode(children = v.values.toSeq))
      }.toMap
    }

    txn.addReadConflictKeys(range.map(_._1))
    for ((k, _) <- range) {
      txn.delete(k)
    }

    BuildResult(
      numKeys = range.size,
      nextCursor = Some(
        range.last._1
          .drop(MerkleTreeTxn.rawMerkleTreeHashBufferPrefix.size)
      )
    )
  }
}
