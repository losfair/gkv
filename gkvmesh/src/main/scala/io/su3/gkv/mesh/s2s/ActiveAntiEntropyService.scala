package io.su3.gkv.mesh.s2s

import io.su3.gkv.mesh.storage.DistributedLock
import io.su3.gkv.mesh.background.UniqueBackgroundService
import io.su3.gkv.mesh.storage.MerkleTreeIncrementalBuilder

object ActiveAntiEntropyService extends UniqueBackgroundService {
  override def serviceName: String = "ActiveAntiEntropyService"
  override def runForever(lock: DistributedLock): Unit = {
    val tkv = lock.tkv

    while (true) {
      MerkleTreeIncrementalBuilder.runIncrementalBuild(lock)

      Thread.sleep(5000)
    }
  }
}
