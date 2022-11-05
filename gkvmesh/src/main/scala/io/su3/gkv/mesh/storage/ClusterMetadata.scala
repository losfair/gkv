package io.su3.gkv.mesh.storage

import com.apple.foundationdb.tuple.Tuple
import scala.collection.mutable.WeakHashMap
import java.util.concurrent.locks.ReentrantReadWriteLock

object ClusterMetadata {
  private val clusterIdKey =
    Tuple.from(TkvKeyspace.clusterMetadataPrefix, "clusterId").pack()
  private val clusterIdCache: WeakHashMap[Tkv, String] = WeakHashMap()
  private val clusterIdCacheLock = ReentrantReadWriteLock(true)

  def initClusterId(tkv: Tkv): Unit = {
    tkv.transact { txn =>
      if (txn.get(clusterIdKey).isEmpty) {
        txn.put(clusterIdKey, java.util.UUID.randomUUID().toString.getBytes())
      }
    }
  }

  def getClusterId(tkv: Tkv): String = {
    clusterIdCacheLock.readLock().lock()
    try {
      clusterIdCache.get(tkv) match {
        case Some(clusterId) => return clusterId
        case None            =>
      }
    } finally {
      clusterIdCacheLock.readLock().unlock()
    }

    val clusterId = tkv.transact { txn =>
      String(
        txn
          .get(clusterIdKey)
          .getOrElse(
            throw new IllegalStateException("Cluster ID not initialized")
          )
      )
    }

    clusterIdCacheLock.writeLock().lock()
    clusterIdCache.put(tkv, clusterId)
    clusterIdCacheLock.writeLock().unlock()

    clusterId
  }
}
