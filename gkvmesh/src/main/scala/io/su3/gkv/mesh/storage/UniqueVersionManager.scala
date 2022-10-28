package io.su3.gkv.mesh.storage

import java.util.concurrent.atomic.AtomicLong
import io.su3.gkv.mesh.proto.persistence.UniqueVersion
import com.google.protobuf.ByteString

object UniqueVersionManager {
  private val nodeId = Array.fill(16)(0.toByte)
  java.security.SecureRandom().nextBytes(nodeId)

  private val opSeq = AtomicLong()

  def next(): UniqueVersion = {
    UniqueVersion(
      realTimestamp = System.currentTimeMillis(),
      nodeId = ByteString.copyFrom(nodeId),
      opSeq = opSeq.incrementAndGet()
    )
  }
}
