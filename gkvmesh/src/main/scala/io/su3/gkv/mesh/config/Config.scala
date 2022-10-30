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
  lazy val logLevel: String = tryGetProperty(
    "gkvmesh.log.level",
    {
      case Some(x) => x
      case None    => "info"
    }
  )
}
