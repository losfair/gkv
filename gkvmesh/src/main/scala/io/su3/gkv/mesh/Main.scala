package io.su3.gkv.mesh

import io.su3.gkv.mesh.storage.Tkv
import io.su3.gkv.mesh.s2s.MeshServer
import com.typesafe.scalalogging.Logger
import java.util.concurrent.Semaphore
import io.su3.gkv.mesh.s2s.ActiveAntiEntropyService
import io.su3.gkv.mesh.background.UniqueBackgroundService
import io.su3.gkv.mesh.config.Config
import io.su3.gkv.mesh.httpapi.ApiServer

private val logger = Logger("Main")

private class BackgroundTaskSet(tkv: Tkv) {
  val services: Seq[Thread] = Seq[UniqueBackgroundService](
    ActiveAntiEntropyService
  ).map { x => UniqueBackgroundService.spawn(tkv, x) }

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

@main def main: Unit =
  org.slf4j.LoggerFactory
    .getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)
    .asInstanceOf[ch.qos.logback.classic.Logger]
    .setLevel(ch.qos.logback.classic.Level.toLevel(Config.logLevel))

  try {
    guard(Tkv(Config.tkvPrefix), _.close()) { tkv =>
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
  } finally {
    logger.info("Node stopped")
  }
