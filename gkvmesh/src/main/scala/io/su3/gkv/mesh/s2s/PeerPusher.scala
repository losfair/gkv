package io.su3.gkv.mesh.s2s

import io.su3.gkv.mesh.storage.Tkv
import io.su3.gkv.mesh.proto.persistence.PeerInfo
import com.typesafe.scalalogging.Logger
import io.su3.gkv.mesh.proto.s2s.PushLeafRequest
import io.su3.gkv.mesh.util.WorkerGroup
import io.su3.gkv.mesh.proto.s2s.MeshGrpc
import io.grpc.ManagedChannelBuilder
import io.su3.gkv.mesh.proto.s2s.MeshGrpc.MeshBlockingStub
import io.su3.gkv.mesh.proto.s2s.Leaf
import scala.collection.mutable.ArrayBuffer
import com.google.gson.Gson
import com.google.gson.stream.JsonReader
import io.su3.gkv.mesh.config.MeshServiceConfig
import io.su3.gkv.mesh.engine.ManagedTask

class PeerPusher(tkv: Tkv, peerClusterId: String, peer: PeerInfo)
    extends ManagedTask {
  private val queueSize = 2048
  private val workerGroupSize = 256
  private val leafBufferSize = 10

  val inbox = new java.util.concurrent.LinkedBlockingQueue[Leaf](queueSize)
  val wg = WorkerGroup("peer-pusher-" + peerClusterId, workerGroupSize)

  override def run(): Unit = {
    val channel = ManagedChannelBuilder
      .forTarget(peer.address)
      .usePlaintext()
      .defaultServiceConfig(MeshServiceConfig.serviceConfig)
      .enableRetry()
      .build()
    val stub = MeshGrpc.blockingStub(channel)

    while (true) {
      val leafBuffer = ArrayBuffer[Leaf]()
      leafBuffer.sizeHint(leafBufferSize)

      val firstLeaf = inbox.take()
      leafBuffer += firstLeaf

      var hasNext = true
      while (hasNext && leafBuffer.length < leafBufferSize) {
        Option(inbox.poll()) match {
          case Some(leaf) => leafBuffer += leaf
          case None       => hasNext = false
        }
      }

      wg.spawn {
        runPushLeaf(stub, PushLeafRequest(entries = leafBuffer.toSeq))
      }
    }
  }

  private def runPushLeaf(
      stub: MeshBlockingStub,
      request: PushLeafRequest
  ): Unit = {
    stub.pushLeaf(request)
  }
}
