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
      key: Array[Byte],
      value: Array[Byte],
      version: Option[UniqueVersion] = None
  ): Option[UniqueVersion] = {
    putHashBuffer(key, version) match {
      case Some(actualVersion) =>
        txn.put(MerkleTreeTxn.rawDataPrefix ++ key, value)
        Some(actualVersion)
      case None => None
    }
  }

  def delete(
      key: Array[Byte],
      version: Option[UniqueVersion] = None
  ): Option[UniqueVersion] = {
    putHashBuffer(key, version) match {
      case Some(actualVersion) =>
        txn.delete(MerkleTreeTxn.rawDataPrefix ++ key)
        Some(actualVersion)
      case None => None
    }
  }

  private def putHashBuffer(
      key: Array[Byte],
      version: Option[UniqueVersion]
  ): Option[UniqueVersion] = {
    val hash = MerkleTreeTxn.hashDataKey(key)

    val bufferedVersionFut =
      txn
        .asyncGet(MerkleTreeTxn.rawMerkleTreeHashBufferPrefix ++ hash)
        .thenApply(
          _.map(MerkleLeaf.parseFrom(_))
            .flatMap(_.version)
            .map(UniqueVersionUtil.serializeUniqueVersion(_).toSeq)
        )
    val persistedVersionFut =
      txn
        .asyncGet(TkvKeyspace.constructMerkleTreeStructureKey(hash.toSeq))
        .thenApply(
          _.flatMap(MerkleNode.parseFrom(_).leaf)
            .flatMap(_.version)
            .map(UniqueVersionUtil.serializeUniqueVersion(_).toSeq)
        )

    val bufferedVersion = bufferedVersionFut.join()
    val persistedVersion = persistedVersionFut.join()
    val maxExistingVersion =
      Seq(Some(Seq.empty[Byte]), bufferedVersion, persistedVersion).flatten.max(
        UnsignedBytesOrdering()
      )

    val ourVersion = version.getOrElse(UniqueVersionManager.next())
    if (
      BytesUtil.compare(
        UniqueVersionUtil.serializeUniqueVersion(ourVersion).toSeq,
        maxExistingVersion
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

  def mergeLeaf(leaf: Leaf): Unit = {
    if (leaf.deleted) {
      delete(leaf.key.toByteArray, Some(leaf.version.get))
    } else {
      put(leaf.key.toByteArray, leaf.value.toByteArray, Some(leaf.version.get))
    }
  }
}

object MerkleTreeTxn {
  val rawDataPrefix = Tuple.from(TkvKeyspace.dataPrefix).pack()
  val rawMerkleTreeHashBufferPrefix =
    Tuple.from(TkvKeyspace.merkleTreeHashBufferPrefix).pack()

  def hashDataKey(key: Array[Byte]): Array[Byte] = {
    MessageDigest.getInstance("SHA-512/256").digest(key)
  }
}
