package io.su3.gkv.mesh.config

import scala.util.control.NonFatal.apply
import scala.util.control.NonFatal
import org.apache.commons.codec.binary.Hex

object Config {
  private def tryGetProperty[T](
      name: String,
      convert: Option[String] => T
  ): T = {
    val value = Option(System.getProperty(name))
    try {
      convert(value)
    } catch {
      case NonFatal(e) =>
        throw new Exception(s"Property $name has an invalid value: $value", e)
    }
  }

  private def mustGetProperty[T](name: String, convert: String => T): T = {
    tryGetProperty(
      name,
      { x =>
        convert(x.getOrElse(throw new Exception(s"Property $name is not set")))
      }
    )
  }

  lazy val tkvPrefix: Array[Byte] =
    mustGetProperty("gkvmesh.tkv.prefixHex", Hex.decodeHex(_))
  lazy val meshserverPort: Int =
    mustGetProperty("gkvmesh.meshserver.port", _.toInt)
  lazy val httpapiPort: Int = mustGetProperty("gkvmesh.httpapi.port", _.toInt)
  lazy val fdbBuggify: Boolean =
    tryGetProperty(
      "gkvmesh.fdb.buggify",
      { x => x.map(_.toBoolean).getOrElse(false) }
    )
  lazy val globalLogLevel: String = tryGetProperty(
    "gkvmesh.log.globalLevel",
    {
      case Some(x) => x
      case None    => "info"
    }
  )
  lazy val localLogLevel: String = tryGetProperty(
    "gkvmesh.log.localLevel",
    {
      case Some(x) => x
      case None    => "info"
    }
  )
  lazy val backgroundTaskPlacement: Map[String, Int] = tryGetProperty(
    "gkvmesh.backgroundTask.placement",
    {
      case Some(x) =>
        x.split(",")
          .map { x =>
            val parts = x.split(":")
            if (parts.length != 2) {
              throw new IllegalArgumentException(
                s"Invalid backgroundTaskPlacement(taskName, priority): $x"
              )
            }
            (parts(0), parts(1).toInt)
          }
          .toMap
      case None => Map()
    }
  )

  lazy val disablePush: Boolean = tryGetProperty(
    "gkvmesh.apiserver.disablePush",
    {
      case Some(x) => x.toBoolean
      case None    => false
    }
  )

  lazy val gclockNtpServer: String = tryGetProperty(
    "gkvmesh.gclock.ntpServer",
    {
      case Some(x) => x
      case None    => "time.google.com"
      // case None    => "169.254.169.123" // AWS TimeSync
    }
  )

  lazy val gclockMaxDispersionMs: Long = tryGetProperty(
    "gkvmesh.gclock.maxDispersionMs",
    {
      case Some(x) => x.toLong
      case None    => 100
    }
  )

  lazy val gclockLocalAccumulatedDispersionPerMs: Double = tryGetProperty(
    "gkvmesh.gclock.localAccumulatedDispersionPerMs",
    {
      case Some(x) => x.toDouble
      case None    => 0.001 // 0.001ms accumulated per ms
    }
  )
}
