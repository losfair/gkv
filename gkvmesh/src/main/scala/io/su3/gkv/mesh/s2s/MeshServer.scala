package io.su3.gkv.mesh.s2s

import io.su3.gkv.mesh.proto.s2s.MeshGrpc
import io.su3.gkv.mesh.proto.s2s.FetchLeafResponse
import io.su3.gkv.mesh.proto.s2s.ExchangeMerkleTreeResponse
import io.su3.gkv.mesh.proto.s2s.ExchangeMerkleTreeRequest
import io.su3.gkv.mesh.proto.s2s.FetchLeafRequest
import io.su3.gkv.mesh.proto.s2s.PushLeafRequest
import io.su3.gkv.mesh.proto.s2s.PushLeafResponse
import scala.concurrent.Future
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext
import io.su3.gkv.mesh.storage.Tkv
import io.grpc.Server
import com.typesafe.scalalogging.Logger
import io.su3.gkv.mesh.config.Config
import io.grpc.netty.NettyServerBuilder

private implicit val defaultExecutionContext: ExecutionContext =
  ExecutionContext.fromExecutorService(
    Executors.newVirtualThreadPerTaskExecutor()
  )

class MeshImpl(val tkv: Tkv) extends MeshGrpc.Mesh {

  override def exchangeMerkleTree(
      request: ExchangeMerkleTreeRequest
  ): Future[ExchangeMerkleTreeResponse] = Future {
    ???
  }

  override def fetchLeaf(request: FetchLeafRequest): Future[FetchLeafResponse] =
    Future { ??? }
  override def pushLeaf(request: PushLeafRequest): Future[PushLeafResponse] =
    Future { ??? }
}

class MeshServer(val tkv: Tkv) {
  private[this] var server: Option[Server] = None
  private val logger = Logger(getClass())

  def start(): Unit = {
    val port = Config.meshserverPort
    server = Some(
      NettyServerBuilder
        .forPort(port)
        .addService(
          MeshGrpc.bindService(new MeshImpl(tkv), defaultExecutionContext)
        )
        .build
        .start
    )
    logger.info(
      "MeshServer started, listening on {}",
      port
    )
  }

  def close(): Unit = {
    server match {
      case Some(x) =>
        x.shutdown()
        x.awaitTermination()
        logger.info("MeshServer closed")
      case None =>
    }
  }
}
