package io.su3.gkv.mesh.util

import io.su3.gkv.mesh.proto.persistence.MerkleNode
import java.nio.ByteBuffer
import io.su3.gkv.mesh.proto.persistence.MerkleLeaf

object MerkleTreeUtil {
  private val interiorNodePayloadPrefix = Array[Byte](0)
  private val leafNodePayloadPrefix = Array[Byte](1)

  def hashNode(node: MerkleNode): Array[Byte] = {
    node.leaf match {
      case Some(value) =>
        return hashLeaf(value)
      case None =>
    }

    val sorted = node.children.sortBy(_.index)
    val hasher = java.security.MessageDigest.getInstance("SHA-512/256")
    var buf = ByteBuffer.allocate(4)

    hasher.update(interiorNodePayloadPrefix)

    for (child <- sorted) {
      if (child.hash.size() != 32) {
        throw new IllegalArgumentException("child hash must be 32 bytes")
      }
      buf.clear()
      buf.putInt(child.index)
      hasher.update(buf.array())
      hasher.update(child.hash.toByteArray())
    }

    hasher.digest()
  }

  private def hashLeaf(leaf: MerkleLeaf): Array[Byte] = {
    val hasher = java.security.MessageDigest.getInstance("SHA-512/256")
    hasher.update(leafNodePayloadPrefix)
    hasher.update(UniqueVersionUtil.serializeUniqueVersion(leaf.version.get))
    hasher.digest()
  }
}
