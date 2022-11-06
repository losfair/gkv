package io.su3.gkv.mesh

import io.su3.gkv.mesh.storage.Tkv
import io.su3.gkv.mesh.s2s.MeshServer
import com.typesafe.scalalogging.Logger
import java.util.concurrent.Semaphore
import io.su3.gkv.mesh.s2s.ActiveAntiEntropyService
import io.su3.gkv.mesh.background.UniqueBackgroundService
import io.su3.gkv.mesh.config.Config
import io.su3.gkv.mesh.httpapi.ApiServer
import io.su3.gkv.mesh.storage.ClusterMetadata
import io.su3.gkv.mesh.s2s.MeshMetadata
import sun.misc.Signal
import sun.misc.SignalHandler

private val logger = Logger("Main")

private class BackgroundTaskSet(tkv: Tkv) {
  val placement = Config.backgroundTaskPlacement

  val services: Seq[Thread] = Seq[UniqueBackgroundService](
    ActiveAntiEntropyService
  ).flatMap { s =>
    if (placement.isEmpty) {
      Some((s, 10))
    } else {
      placement.get(s.serviceName) match {
        case Some(n) => Some((s, n))
        case None    => None
      }
    }
  }.map { case (service, priority) =>
    logger.info(
      "Placed background service '{}' with priority {}",
      service.serviceName,
      priority
    )
    UniqueBackgroundService.spawn(tkv, service, priority)
  }

  def close(): Unit = {
    services.foreach(_.interrupt())
    services.foreach(_.join())
  }
}

private def guard[T, R](init: => T, close: T => Unit)(body: T => R): R = {
  val obj = init
  try {
    body(obj)
  } finally {
    close(obj)
  }
}

def realMain: Unit =
  try {
    guard(Tkv(Config.tkvPrefix), _.close()) { tkv =>
      ClusterMetadata.initClusterId(tkv)
      val clusterId = ClusterMetadata.getClusterId(tkv)
      logger.info("Cluster ID: {}", clusterId)

      // Initialization order: MeshMetadata must be closed after ApiServer
      guard(MeshMetadata(tkv), _.close()) { meshMetadata =>
        meshMetadata.start()
        guard(MeshServer(tkv), _.close()) { meshServer =>
          meshServer.start()
          guard(ApiServer(tkv), _.close()) { apiServer =>
            apiServer.start()
            guard(BackgroundTaskSet(tkv), _.close()) { bgTaskSet =>
              logger.info("Node started")
              Semaphore(0).acquire()
            }
          }
        }
      }
    }
  } finally {
    logger.info("Node stopped")
  }

@main def main(): Unit =
  org.slf4j.LoggerFactory
    .getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)
    .asInstanceOf[ch.qos.logback.classic.Logger]
    .setLevel(ch.qos.logback.classic.Level.toLevel(Config.globalLogLevel))
  org.slf4j.LoggerFactory
    .getLogger("io.su3.gkv.mesh")
    .asInstanceOf[ch.qos.logback.classic.Logger]
    .setLevel(ch.qos.logback.classic.Level.toLevel(Config.localLogLevel))

  if (Thread.currentThread().threadId() != 1) {
    logger.info("Running in SBT task")
    realMain
  } else {
    logger.info("Running in JVM main thread")
    val mainThread = Thread.currentThread()

    for (sig <- Seq("INT", "TERM")) {
      Signal.handle(
        Signal(sig),
        new SignalHandler {
          override def handle(sig: Signal): Unit = {
            logger.info("Received signal: {}", sig.getName())
            mainThread.interrupt()
          }
        }
      )
    }

    try {
      realMain
    } catch {
      case _: InterruptedException =>
    }
  }
