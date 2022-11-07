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
import io.su3.gkv.mesh.util.BytesUtil
import scala.collection.mutable.Queue
import scala.collection.mutable.HashSet
import io.su3.gkv.mesh.util.BytesUtil.UnsignedBytesOrdering
import scala.collection.mutable.TreeSet

object MerkleTreeIncrementalBuilder {
  private val lastHash = Array.fill(32)(0xff.toByte)
  private val logger = Logger(getClass())

  private case class BuildResult(numKeys: Int, nextCursor: Option[Array[Byte]])

  def runIncrementalBuild(lock: DistributedLock): Unit = {
    var hashCursor: Option[Array[Byte]] = Some(Array.fill(32)(0.toByte))

    while (true) {
      hashCursor match {
        case Some(x) =>
          var newRoot: Option[MerkleNode] = None
          var firstTry = true
          val startTime = System.currentTimeMillis()
          val result = lock.tkv.transact { txn =>
            if (firstTry) {
              firstTry = false
            } else {
              logger.warn("Retrying incremental build")
            }
            lock.validate(txn)
            val result = runIncrementalBuildOnce(txn, x, 1000)
            newRoot = txn
              .get(
                TkvKeyspace.constructMerkleTreeStructureKey(Seq()),
                TkvTxnReadMode.Snapshot
              )
              .map { x => MerkleNode.parseFrom(x) }
            result
          }
          val endTime = System.currentTimeMillis()
          logger.info(
            "from: {} numKeys: {} newRoot: {} duration: {}ms",
            Hex.encodeHexString(x),
            result.numKeys,
            newRoot
              .map { x => Hex.encodeHexString(MerkleTreeUtil.hashNode(x)) }
              .getOrElse("-"),
            endTime - startTime
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

    var txnPutCount = 0

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

    val dirtyNodes = loadAndExpandDirtyNodes(txn, range)

    var propagatedUpdates: Map[Seq[Byte], MerkleNode] = range.map {
      case (k, v) =>
        // Write level 32 - leaf
        val hash =
          k.drop(MerkleTreeTxn.rawMerkleTreeHashBufferPrefix.length).toSeq
        val leaf = MerkleLeaf.parseFrom(v)
        (
          hash,
          MerkleNode(children = Seq(), leaf = Some(leaf))
        )
    }.toMap

    for (i <- Range.inclusive(32, 0, -1)) {
      val children =
        HashMap[Seq[Byte], HashMap[Int, (MerkleChild, Option[MerkleNode])]]()

      for ((k, v) <- propagatedUpdates) {
        assert(k.length == i)
        if (i != 0) {
          val hashPrefix = k.take(i - 1)
          val index = k(i - 1).toInt & 0xff
          val current = dirtyNodes.get(hashPrefix).get

          val map = children.getOrElseUpdate(
            hashPrefix,
            current
              .map { x =>
                HashMap.from(x.children.map { c => (c.index, (c, None)) })
              }
              .getOrElse(HashMap())
          )
          val child = MerkleChild(
            index = index,
            hash = ByteString.copyFrom(MerkleTreeUtil.hashNode(v))
          )
          map.put(child.index, (child, Some(v)))
        } else {
          // Root node
          txn.put(
            TkvKeyspace.constructMerkleTreeStructureKey(k),
            v.toByteArray
          )
          txnPutCount += 1
        }
      }

      // XXX: Side effect: txn.put
      propagatedUpdates = children.map { case (k, v) =>
        val canCompress = i != 32 && v.values.size == 1
        (
          k,
          MerkleNode(children = v.values.map {
            case (child, Some(node)) =>
              if (canCompress) {
                child.copy(inlineNode = Some(node))
              } else {
                txn.put(
                  TkvKeyspace.constructMerkleTreeStructureKey(
                    k :+ child.index.toByte
                  ),
                  node.toByteArray
                )
                txnPutCount += 1
                child
              }
            case (child, None) => child
          }.toSeq)
        )
      }.toMap
    }

    // Atomic compare-and-delete during commit.
    // This prevents conflict with online transactions.
    for ((k, v) <- range) {
      txn.compareAndDelete(k, v)
      txnPutCount += 1
    }

    logger.info("runIncrementalBuildOnce: txnPutCount: {}", txnPutCount)

    BuildResult(
      numKeys = range.size,
      nextCursor = Some(
        range.last._1
          .drop(MerkleTreeTxn.rawMerkleTreeHashBufferPrefix.size)
      )
    )
  }

  private def loadAndExpandDirtyNodes(
      txn: TkvTxn,
      range: Seq[(Array[Byte], Array[Byte])]
  ): Map[Seq[Byte], Option[MerkleNode]] = {
    val pendingDataKeys = HashSet[Seq[Byte]]()
    for ((k, _) <- range) {
      pendingDataKeys.add(
        k.drop(MerkleTreeTxn.rawMerkleTreeHashBufferPrefix.length).toSeq
      )
    }

    val dirtyNodes = HashMap[Seq[Byte], Option[MerkleNode]]()
    var txnGetCount = 0

    for (j <- Range.inclusive(0, 28, 4)) {
      val dirtyNodeFutures =
        HashMap[Seq[Byte], CompletableFuture[Option[MerkleNode]]]()
      for (i <- Range.inclusive(j, if (j == 28) j + 4 else j + 3, 1)) {
        for (k <- pendingDataKeys) {
          val hashPrefix = k.take(i).toSeq
          if (
            !dirtyNodes.contains(hashPrefix) &&
            !dirtyNodeFutures.contains(hashPrefix)
          ) {
            val fut =
              txn.asyncGet(
                TkvKeyspace.constructMerkleTreeStructureKey(hashPrefix),
                TkvTxnReadMode.Snapshot
              )
            dirtyNodeFutures.put(
              hashPrefix,
              fut.thenApply { x =>
                x.map { x =>
                  MerkleNode.parseFrom(x)
                }
              }
            )
            txnGetCount += 1
          }
        }
      }
      val localDirtyNodes =
        HashMap.from(dirtyNodeFutures.iterator.map({ x =>
          (x._1, x._2.join())
        }))
      expandDirtyNodes(localDirtyNodes)
      expandEmptyDirtyNodes(localDirtyNodes, pendingDataKeys)
      dirtyNodes ++= localDirtyNodes
    }

    logger.info("loadAndExpandDirtyNodes: txnGetCount: {}", txnGetCount)
    dirtyNodes.toMap
  }

  private def expandEmptyDirtyNodes(
      nodes: HashMap[Seq[Byte], Option[MerkleNode]],
      pendingDataKeys: HashSet[Seq[Byte]]
  ): Unit = {
    val pendingDataKeysTree: TreeSet[Seq[Byte]] = TreeSet
      .from(pendingDataKeys)(UnsignedBytesOrdering())

    for ((k, v) <- nodes.iterator.filter(_._2.isEmpty)) {
      val it =
        pendingDataKeysTree.iteratorFrom(k).takeWhile(_.startsWith(k))
      val pendingDelete = ArrayBuffer[Seq[Byte]]()
      while (it.hasNext) {
        val next: Seq[Byte] = it.next()
        for (i <- Range.inclusive(k.length, 32, 1)) {
          nodes.put(next.take(i), None)
        }
        pendingDelete += next
      }
      for (i <- pendingDelete) {
        pendingDataKeys.remove(i)
        pendingDataKeysTree.remove(i)
      }
    }
  }

  private def expandDirtyNodes(
      nodes: HashMap[Seq[Byte], Option[MerkleNode]]
  ): Unit = {
    val queue = Queue[(Seq[Byte], MerkleNode)]()
    for ((k, v) <- nodes) {
      v.foreach { n =>
        queue.enqueue((k, n))
      }
    }

    while (!queue.isEmpty) {
      val (k, v) = queue.dequeue()
      for (child <- v.children) {
        child.inlineNode.foreach { n =>
          val extendedPrefix = k :+ child.index.toByte
          nodes.put(extendedPrefix, Some(n))
          queue.enqueue((extendedPrefix, n))
        }
      }
    }
  }
}
