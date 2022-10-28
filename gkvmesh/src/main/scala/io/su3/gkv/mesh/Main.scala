package io.su3.gkv.mesh

import io.su3.gkv.mesh.storage.Tkv
import org.apache.commons.codec.binary.Hex
import io.su3.gkv.mesh.s2s.MeshServer
import com.typesafe.scalalogging.Logger
import java.util.concurrent.Semaphore
import io.su3.gkv.mesh.s2s.ActiveAntiEntropyService
import io.su3.gkv.mesh.background.UniqueBackgroundService

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

@main def main: Unit =
  val tkvPrefix = Option(System.getProperty("gkvmesh.tkv.prefixHex"))
    .getOrElse(throw new Exception("gkvmesh.tkv.prefixHex is not set"))
  val tkv = Tkv(Hex.decodeHex(tkvPrefix))
  try {
    val meshServer = MeshServer(tkv)
    try {
      meshServer.start()

      val bgTaskSet = BackgroundTaskSet(tkv)
      try {
        logger.info("Node started")
        Semaphore(0).acquire()
      } finally {
        bgTaskSet.close()
      }
    } finally {
      meshServer.close()
    }
  } finally {
    tkv.close()
    logger.info("Node stopped")
  }
