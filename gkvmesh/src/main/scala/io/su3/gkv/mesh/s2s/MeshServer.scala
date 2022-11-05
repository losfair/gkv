package io.su3.gkv.mesh.s2s

import io.su3.gkv.mesh.proto.s2s.MeshGrpc
import io.su3.gkv.mesh.proto.s2s.PullLeafResponse
import io.su3.gkv.mesh.proto.s2s.PullMerkleTreeResponse
import io.su3.gkv.mesh.proto.s2s.PullMerkleTreeRequest
import io.su3.gkv.mesh.proto.s2s.PullLeafRequest
import io.su3.gkv.mesh.proto.s2s.PushLeafRequest
import io.su3.gkv.mesh.proto.s2s.PushLeafResponse
import scala.concurrent.Future
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext
import io.su3.gkv.mesh.storage.Tkv
import io.grpc.Server
import com.typesafe.scalalogging.Logger
import io.su3.gkv.mesh.config.Config
import io.su3.gkv.mesh.storage.TkvKeyspace
import io.su3.gkv.mesh.proto.persistence.MerkleNode
import io.su3.gkv.mesh.util.MerkleTreeUtil
import java.util.concurrent.CompletableFuture
import io.su3.gkv.mesh.storage.MerkleTreeTxn
import io.su3.gkv.mesh.proto.s2s.Leaf
import com.google.protobuf.ByteString
import io.grpc.ServerBuilder
import io.grpc.netty.NettyServerBuilder

private implicit val defaultExecutionContext: ExecutionContext =
  ExecutionContext.fromExecutorService(
    Executors.newVirtualThreadPerTaskExecutor()
  )

class MeshImpl(val tkv: Tkv) extends MeshGrpc.Mesh {

  override def pullMerkleTree(
      request: PullMerkleTreeRequest
  ): Future[PullMerkleTreeResponse] = Future {
    tkv.transact { txn =>
      val node = txn
        .get(
          TkvKeyspace.constructMerkleTreeStructureKey(
            request.prefix.toByteArray().toSeq
          )
        )
        .map(MerkleNode.parseFrom(_))
        .getOrElse(MerkleNode())
      val actualHash = MerkleTreeUtil.hashNode(node)

      if (actualHash.sameElements(request.hash.toByteArray())) {
        PullMerkleTreeResponse(identical = true)
      } else {
        PullMerkleTreeResponse(identical = false, children = node.children)
      }
    }
  }

  override def pullLeaf(request: PullLeafRequest): Future[PullLeafResponse] =
    Future {
      tkv.transact { txn =>
        val entries = request.hashes
          .map { hash =>
            val key = TkvKeyspace.constructMerkleTreeStructureKey(
              hash.toByteArray().toSeq
            )
            val value = txn
              .asyncGet(key)
              .thenCompose(
                _.map(MerkleNode.parseFrom(_))
                  .flatMap(_.leaf)
                  .map { leaf =>
                    val dataKey =
                      MerkleTreeTxn.rawDataPrefix ++ leaf.key.toByteArray()
                    txn
                      .asyncGet(dataKey)
                      .thenApply[Option[Leaf]]({ dataValue =>
                        Some(
                          Leaf(
                            key = leaf.key,
                            value = ByteString.copyFrom(
                              dataValue.getOrElse(Array.emptyByteArray)
                            ),
                            version = leaf.version,
                            deleted = dataValue.isEmpty
                          )
                        )
                      })
                  }
                  .getOrElse(CompletableFuture.completedFuture(None))
              )
            value
          }
          .flatMap(_.get())
        PullLeafResponse(entries = entries)
      }
    }
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
