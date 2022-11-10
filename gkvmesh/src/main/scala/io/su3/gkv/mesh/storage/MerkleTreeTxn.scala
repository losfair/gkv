package io.su3.gkv.mesh.storage

import com.apple.foundationdb.tuple.Tuple
import java.security.MessageDigest
import io.su3.gkv.mesh.proto.persistence.MerkleLeaf
import com.google.protobuf.ByteString
import io.su3.gkv.mesh.proto.persistence.UniqueVersion
import io.su3.gkv.mesh.proto.s2s.Leaf
import io.su3.gkv.mesh.proto.persistence.MerkleNode
import io.su3.gkv.mesh.util.UniqueVersionUtil
import io.su3.gkv.mesh.util.BytesUtil
import io.su3.gkv.mesh.util.BytesUtil.UnsignedBytesOrdering
import java.util.concurrent.CompletableFuture
import com.typesafe.scalalogging.Logger
import org.apache.commons.codec.binary.Hex
import io.su3.gkv.mesh.gclock.GClock

class MerkleTreeTxn(txn: TkvTxn) {
  def get(key: Array[Byte]): Option[Array[Byte]] = {
    txn.get(MerkleTreeTxn.rawDataPrefix ++ key)
  }

  def range(
      start: Array[Byte],
      end: Array[Byte],
      limit: Int
  ): Seq[(Array[Byte], Array[Byte])] = {
    txn
      .snapshotRange(
        MerkleTreeTxn.rawDataPrefix ++ start,
        MerkleTreeTxn.rawDataPrefix ++ end,
        limit
      )
      .map { x =>
        (x._1.drop(MerkleTreeTxn.rawDataPrefix.length), x._2)
      }
  }

  def put(
      gclock: GClock,
      key: Array[Byte],
      value: Array[Byte],
      version: Option[UniqueVersion] = None
  ): Option[UniqueVersion] = {
    putHashBuffer(gclock, key, version) match {
      case Some(actualVersion) =>
        txn.put(MerkleTreeTxn.rawDataPrefix ++ key, value)
        Some(actualVersion)
      case None => None
    }
  }

  def delete(
      gclock: GClock,
      key: Array[Byte],
      version: Option[UniqueVersion] = None
  ): Option[UniqueVersion] = {
    putHashBuffer(gclock, key, version) match {
      case Some(actualVersion) =>
        txn.delete(MerkleTreeTxn.rawDataPrefix ++ key)
        Some(actualVersion)
      case None => None
    }
  }

  def asyncKeyMetadata(
      dataKeyHash: Seq[Byte]
  ): CompletableFuture[Option[MerkleLeaf]] = {
    val bufferedFut =
      txn
        .asyncGet(
          MerkleTreeTxn.rawMerkleTreeHashBufferPrefix ++ dataKeyHash.toArray
        )
        .thenApply(
          _.map(MerkleLeaf.parseFrom(_))
        )
    val persistedFut =
      txn
        .asyncGet(TkvKeyspace.constructMerkleTreeStructureKey(dataKeyHash))
        .thenApply(
          _.flatMap(MerkleNode.parseFrom(_).leaf)
        )

    bufferedFut.thenCombine(
      persistedFut,
      (buffered, persisted) => {
        if (buffered.isEmpty) {
          persisted
        } else if (persisted.isEmpty) {
          buffered
        } else {
          val (bufferedVersion, persistedVersion) =
            (buffered.get.version.get, persisted.get.version.get)
          if (
            BytesUtil.compare(
              UniqueVersionUtil.serializeUniqueVersion(bufferedVersion),
              UniqueVersionUtil.serializeUniqueVersion(persistedVersion)
            ) > 0
          ) {
            buffered
          } else {
            MerkleTreeTxn.logger.error(
              "Buffered version is not newer than persisted version for data key '{}'",
              Hex.encodeHexString(buffered.get.key.toByteArray)
            )
            persisted
          }
        }
      }
    )
  }

  private def putHashBuffer(
      gclock: GClock,
      key: Array[Byte],
      version: Option[UniqueVersion]
  ): Option[UniqueVersion] = {
    val hash = MerkleTreeTxn.hashDataKey(key)
    val md = asyncKeyMetadata(hash.toSeq).join()
    val existingVersion = md
      .map { x => UniqueVersionUtil.serializeUniqueVersion(x.version.get) }
      .getOrElse(Array.emptyByteArray)

    val ourVersion = version.getOrElse(UniqueVersionManager.next(gclock))
    if (
      BytesUtil.compare(
        UniqueVersionUtil.serializeUniqueVersion(ourVersion).toSeq,
        existingVersion
      ) > 0
    ) {
      txn.put(
        MerkleTreeTxn.rawMerkleTreeHashBufferPrefix ++ hash,
        MerkleLeaf(
          key = ByteString.copyFrom(key),
          version = Some(ourVersion)
        ).toByteArray
      )
      Some(ourVersion)
    } else {
      None
    }
  }

  def mergeLeaf(gclock: GClock, leaf: Leaf): Unit = {
    if (leaf.deleted) {
      delete(gclock, leaf.key.toByteArray, Some(leaf.version.get))
    } else {
      put(gclock, leaf.key.toByteArray, leaf.value.toByteArray, Some(leaf.version.get))
    }
  }
}

object MerkleTreeTxn {
  val rawDataPrefix = Tuple.from(TkvKeyspace.dataPrefix).pack()
  val rawMerkleTreeHashBufferPrefix =
    Tuple.from(TkvKeyspace.merkleTreeHashBufferPrefix).pack()
  val logger = Logger(getClass())

  def hashDataKey(key: Array[Byte]): Array[Byte] = {
    MessageDigest.getInstance("SHA-512/256").digest(key)
  }
}
