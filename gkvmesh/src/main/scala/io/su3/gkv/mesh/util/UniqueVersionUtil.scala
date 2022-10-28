package io.su3.gkv.mesh.util

import io.su3.gkv.mesh.proto.persistence.UniqueVersion
import java.nio.ByteBuffer
import com.google.protobuf.ByteString

object UniqueVersionUtil {
  def serializeUniqueVersion(uv: UniqueVersion): Array[Byte] = {
    if (uv.nodeId.size() != 16) {
      throw new IllegalArgumentException("nodeId must be 16 bytes")
    }

    ByteBuffer
      .allocate(8 + 16 + 8)
      .putLong(uv.realTimestamp)
      .put(uv.nodeId.asReadOnlyByteBuffer)
      .putLong(uv.opSeq)
      .array()
  }

  def deserializeUniqueVersion(uv: Array[Byte]): UniqueVersion = {
    if (uv.size != 8 + 16 + 8) {
      throw new IllegalArgumentException("unique version must be 32 bytes")
    }

    val buf = ByteBuffer.wrap(uv)
    val realTimestamp = buf.getLong()
    val nodeId = ByteString.copyFrom(buf, 16)
    val opSeq = buf.getLong()

    UniqueVersion(
      realTimestamp = realTimestamp,
      nodeId = nodeId,
      opSeq = opSeq
    )
  }
}
