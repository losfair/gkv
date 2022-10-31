package io.su3.gkv.mesh.storage

import com.apple.foundationdb.tuple.Tuple
import java.security.MessageDigest
import io.su3.gkv.mesh.proto.persistence.MerkleLeaf
import com.google.protobuf.ByteString

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

  def put(key: Array[Byte], value: Array[Byte]): Unit = {
    txn.put(MerkleTreeTxn.rawDataPrefix ++ key, value)
    putHashBuffer(key)
  }

  def delete(key: Array[Byte]): Unit = {
    txn.delete(MerkleTreeTxn.rawDataPrefix ++ key)
    putHashBuffer(key)
  }

  private def putHashBuffer(key: Array[Byte]): Unit = {
    val hash = MerkleTreeTxn.hashDataKey(key)
    txn.put(
      MerkleTreeTxn.rawMerkleTreeHashBufferPrefix ++ hash,
      MerkleLeaf(
        key = ByteString.copyFrom(key),
        version = Some(UniqueVersionManager.next())
      ).toByteArray
    )
  }
}

object MerkleTreeTxn {
  val rawDataPrefix = Tuple.from(TkvKeyspace.dataPrefix).pack()
  val rawMerkleTreeHashBufferPrefix =
    Tuple.from(TkvKeyspace.merkleTreeHashBufferPrefix).pack()

  private def hashDataKey(key: Array[Byte]): Array[Byte] = {
    MessageDigest.getInstance("SHA-512/256").digest(key)
  }
}
