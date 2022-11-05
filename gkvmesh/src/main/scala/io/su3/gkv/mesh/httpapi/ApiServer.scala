package io.su3.gkv.mesh.httpapi

import com.typesafe.scalalogging.Logger
import io.su3.gkv.mesh.storage.Tkv
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.ChannelOption
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.logging.LogLevel
import io.netty.channel.ChannelInitializer
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.http.HttpServerCodec
import io.netty.handler.codec.http.HttpServerExpectContinueHandler
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.handler.codec.http.HttpObject
import io.netty.channel.ChannelHandlerContext
import io.su3.gkv.mesh.config.Config
import io.netty.handler.codec.http.HttpRequest
import io.netty.handler.codec.http.DefaultFullHttpResponse
import io.netty.channel.ChannelFutureListener
import io.netty.handler.codec.http.HttpVersion.HTTP_1_1
import io.netty.buffer.Unpooled
import io.netty.handler.codec.http.HttpUtil
import scala.concurrent.ExecutionContext
import java.util.concurrent.Executors
import scala.concurrent.Future
import scala.util.Success
import scala.util.Failure
import io.netty.handler.codec.http.HttpResponseStatus
import io.netty.channel.ChannelPromise
import io.netty.channel.Channel
import io.netty.util.concurrent.GenericFutureListener
import scala.util.Try
import scala.util.Using
import io.su3.gkv.mesh.storage.MerkleTreeTxn
import io.su3.gkv.mesh.proto.httpapi.KvGetRequest
import io.netty.handler.codec.http.HttpObjectAggregator
import io.netty.handler.codec.http.FullHttpRequest
import io.su3.gkv.mesh.proto.httpapi.KvGetResponse
import com.google.protobuf.ByteString
import io.su3.gkv.mesh.proto.httpapi.KvSetRequest
import io.su3.gkv.mesh.proto.httpapi.KvSetResponse
import com.fasterxml.jackson.core.JsonParseException
import java.nio.charset.Charset
import com.fasterxml.jackson.core.JsonProcessingException
import io.su3.gkv.mesh.background.UniqueBackgroundService
import io.su3.gkv.mesh.s2s.ActiveAntiEntropyService
import io.su3.gkv.mesh.proto.httpapi.AaePullRequest
import io.su3.gkv.mesh.proto.httpapi.AaePullResponse
import java.net.SocketException
import io.su3.gkv.mesh.proto.httpapi.KvListRequest
import io.su3.gkv.mesh.proto.httpapi.KvListResponse
import io.su3.gkv.mesh.proto.httpapi.KvListResponseEntry
import io.su3.gkv.mesh.storage.ClusterMetadata
import io.su3.gkv.mesh.storage.MeshMetadata
import io.su3.gkv.mesh.proto.httpapi.PeersResponseEntry
import io.su3.gkv.mesh.proto.httpapi.PeersResponse

private implicit val defaultExecutionContext: ExecutionContext =
  ExecutionContext.fromExecutorService(
    Executors.newVirtualThreadPerTaskExecutor()
  )

class ApiServer(val tkv: Tkv) {
  private var th: Option[Thread] = None

  def start(): Unit = {
    val me = this
    th = Some(Thread.startVirtualThread(new Runnable {
      override def run(): Unit = me.run()
    }))
  }

  def run(): Unit = {
    val bossGroup = new NioEventLoopGroup(1);
    val workerGroup = new NioEventLoopGroup();
    try {
      val b = ServerBootstrap()
      b.option(ChannelOption.SO_BACKLOG, 1024)
      b.group(bossGroup, workerGroup)
        .channel(classOf[NioServerSocketChannel])
        .childHandler(ApiServerInitializer(this))
      val ch = b.bind(Config.httpapiPort).sync().channel()
      ApiServer.logger.info(
        "HTTP API server started on port {}",
        Config.httpapiPort
      )
      ch.closeFuture().sync()
    } finally {
      val bossShutdown = bossGroup.shutdownGracefully()
      val workerShutdown = workerGroup.shutdownGracefully()
      bossShutdown.sync()
      workerShutdown.sync()
      ApiServer.logger.info("HTTP API server stopped")
    }
  }

  def close(): Unit = {
    th match {
      case Some(t) =>
        t.interrupt()
        t.join()
      case None => ()
    }
  }

  private[httpapi] def handleRequest(
      req: FullHttpRequest
  ): DefaultFullHttpResponse = {
    req.uri() match {
      case "/data/get" =>
        // KV get
        val reqBody = scalapb.json4s.JsonFormat
          .fromJsonString[KvGetRequest](
            req.content().toString(Charset.defaultCharset())
          )
        val value = tkv.transact { txn =>
          MerkleTreeTxn(txn).get(reqBody.key.toByteArray())
        }
        val resBody = scalapb.json4s.JsonFormat.toJsonString(
          KvGetResponse(
            value = value
              .map { x => ByteString.copyFrom(x) }
              .getOrElse(ByteString.EMPTY),
            exists = value.isDefined
          )
        )
        DefaultFullHttpResponse(
          req.protocolVersion(),
          HttpResponseStatus.OK,
          Unpooled.wrappedBuffer(resBody.getBytes())
        )
      case "/data/set" =>
        // KV set
        val reqBody = scalapb.json4s.JsonFormat
          .fromJsonString[KvSetRequest](
            req.content().toString(Charset.defaultCharset())
          )
        tkv.transact { txn =>
          // Ensure FDB rate limit has a chance to kick in
          txn.getReadVersion()

          if (reqBody.delete) {
            MerkleTreeTxn(txn).delete(reqBody.key.toByteArray())
          } else {
            MerkleTreeTxn(txn).put(
              reqBody.key.toByteArray(),
              reqBody.value.toByteArray()
            )
          }
        }
        val resBody = scalapb.json4s.JsonFormat.toJsonString(
          KvSetResponse()
        )
        DefaultFullHttpResponse(
          req.protocolVersion(),
          HttpResponseStatus.OK,
          Unpooled.wrappedBuffer(resBody.getBytes())
        )
      case "/data/list" =>
        // KV list
        val reqBody = scalapb.json4s.JsonFormat
          .fromJsonString[KvListRequest](
            req.content().toString(Charset.defaultCharset())
          )
        val entries = tkv.transact { txn =>
          MerkleTreeTxn(txn).range(
            reqBody.start.toByteArray(),
            reqBody.end.toByteArray(),
            reqBody.limit
          )
        }
        val resBody = scalapb.json4s.JsonFormat.toJsonString(
          KvListResponse(
            entries = entries.map { case (k, v) =>
              KvListResponseEntry(
                key = ByteString.copyFrom(k),
                value = ByteString.copyFrom(v)
              )
            }
          )
        )
        DefaultFullHttpResponse(
          req.protocolVersion(),
          HttpResponseStatus.OK,
          Unpooled.wrappedBuffer(resBody.getBytes())
        )
      case "/control/aae/pull" => {
        // AAE pull from peer
        Thread.currentThread().setName("aae-pull")
        val reqBody = scalapb.json4s.JsonFormat
          .fromJsonString[AaePullRequest](
            req.content().toString(Charset.defaultCharset())
          )
        val lock =
          UniqueBackgroundService.takeover(tkv, ActiveAntiEntropyService, -1) {
            lock =>
              ActiveAntiEntropyService.runOnce(lock, Seq(reqBody.peer))
          }
        val resBody = scalapb.json4s.JsonFormat.toJsonString(
          AaePullResponse()
        )
        DefaultFullHttpResponse(
          req.protocolVersion(),
          HttpResponseStatus.OK,
          Unpooled.wrappedBuffer(resBody.getBytes())
        )
      }
      case "/cluster_id" => {
        DefaultFullHttpResponse(
          req.protocolVersion(),
          HttpResponseStatus.OK,
          Unpooled.wrappedBuffer(
            (ClusterMetadata.getClusterId(tkv) + "\n").getBytes()
          )
        )
      }
      case "/peers" => {
        val ss = MeshMetadata.get.snapshot
        val resBody = scalapb.json4s.JsonFormat.toJsonString(
          PeersResponse(
            peers = ss.peers.map { case (clusterId, peer) =>
              PeersResponseEntry(
                clusterId = clusterId,
                address = peer.address
              )
            }.toSeq
          )
        )
        DefaultFullHttpResponse(
          req.protocolVersion(),
          HttpResponseStatus.OK,
          Unpooled.wrappedBuffer(resBody.getBytes())
        )
      }
      case _ =>
        DefaultFullHttpResponse(
          req.protocolVersion(),
          HttpResponseStatus.NOT_FOUND,
          Unpooled.EMPTY_BUFFER
        )
    }
  }
}

object ApiServer {
  val logger = Logger(getClass())
}

private class ApiServerInitializer(server: ApiServer)
    extends ChannelInitializer[SocketChannel] {
  override protected def initChannel(ch: SocketChannel): Unit = {
    val p = ch.pipeline()
    p.addLast(
      HttpServerCodec(),
      HttpServerExpectContinueHandler(),
      HttpObjectAggregator(1048576),
      ApiServerHandler(server)
    )
  }
}

private class ApiServerHandler(server: ApiServer)
    extends SimpleChannelInboundHandler[HttpObject] {
  override protected def channelRead0(
      ctx: ChannelHandlerContext,
      msg: HttpObject
  ): Unit = {
    if (msg.isInstanceOf[FullHttpRequest]) {
      val req = msg.asInstanceOf[FullHttpRequest]
      val keepAlive = HttpUtil.isKeepAlive(req)
      val promise =
        ctx.channel().eventLoop().newPromise[Try[DefaultFullHttpResponse]]()

      req.retain()
      Thread.startVirtualThread(new Runnable {
        override def run(): Unit = {
          promise.setSuccess(Try(server.handleRequest(req)))
          req.release()
        }
      })

      promise.addListener(
        new GenericFutureListener[
          io.netty.util.concurrent.Future[Try[DefaultFullHttpResponse]]
        ] {
          override def operationComplete(
              future: io.netty.util.concurrent.Future[Try[
                DefaultFullHttpResponse
              ]]
          ): Unit = {
            val res = future.getNow() match {
              case Success(res) => res
              case Failure(e)
                  if e.isInstanceOf[JsonProcessingException] || e
                    .isInstanceOf[scalapb.json4s.JsonFormatException] =>
                DefaultFullHttpResponse(
                  req.protocolVersion(),
                  HttpResponseStatus.BAD_REQUEST,
                  Unpooled.wrappedBuffer((e.getMessage() + "\n").getBytes())
                )
              case Failure(e) =>
                ApiServer.logger.error("Error while processing request", e)
                DefaultFullHttpResponse(
                  req.protocolVersion(),
                  HttpResponseStatus.INTERNAL_SERVER_ERROR,
                  Unpooled.wrappedBuffer("Error".getBytes())
                )
            }

            res
              .headers()
              .setInt("Content-Length", res.content().readableBytes())
            if (keepAlive) {
              if (!req.protocolVersion().isKeepAliveDefault()) {
                res.headers().set("Connection", "keep-alive")
              }
            } else {
              res.headers().set("Connection", "close")
            }

            val writeFut = ctx.writeAndFlush(res)
            if (!keepAlive) {
              writeFut.addListener(ChannelFutureListener.CLOSE)
            }
          }
        }
      )
    }
  }

  override def channelReadComplete(ctx: ChannelHandlerContext): Unit = {
    ctx.flush()
  }

  override def exceptionCaught(
      ctx: ChannelHandlerContext,
      cause: Throwable
  ): Unit = {
    cause match {
      case _: SocketException =>
      case _ =>
        ApiServer.logger.error("ApiServer exception", cause)
    }
    ctx.close()
  }
}
