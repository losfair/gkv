package io.su3.gkv.mesh.s2s

import io.su3.gkv.mesh.storage.DistributedLock
import io.su3.gkv.mesh.background.UniqueBackgroundService
import io.su3.gkv.mesh.storage.MerkleTreeIncrementalBuilder
import java.security.SecureRandom
import io.grpc.ManagedChannelBuilder
import com.typesafe.scalalogging.Logger

object ActiveAntiEntropyService extends UniqueBackgroundService {
  private val rng = SecureRandom()
  private val logger = Logger(getClass())

  override def serviceName: String = "ActiveAntiEntropyService"
  override def runForever(lock: DistributedLock): Unit = {
    while (true) {
      runOnce(lock)
      val jitter = rng.nextLong(1000)
      Thread.sleep(5000 + jitter)
    }
  }

  def runOnce(lock: DistributedLock): Unit = {
    MerkleTreeIncrementalBuilder.runIncrementalBuild(lock)
  }

  private def pullFromPeer(lock: DistributedLock, peerAddress: String): Unit = {
    val channel =
      ManagedChannelBuilder.forTarget(peerAddress).usePlaintext().build()

  }
}
