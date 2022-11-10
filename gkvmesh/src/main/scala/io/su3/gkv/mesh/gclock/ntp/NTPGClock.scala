package io.su3.gkv.mesh.gclock.ntp

import io.su3.gkv.mesh.gclock.GClock
import java.util.concurrent.atomic.AtomicReference
import org.apache.commons.net.ntp.NTPUDPClient
import java.net.InetAddress
import com.typesafe.scalalogging.Logger
import io.su3.gkv.mesh.engine.ManagedTask
import scala.util.control.NonFatal.apply
import scala.util.control.NonFatal
import io.su3.gkv.mesh.config.Config

class NTPGClock extends ManagedTask, GClock {
  private val clockUncertaintyMs = 2

  private val ntpServer = Config.gclockNtpServer
  private val maxDispersionMs = Config.gclockMaxDispersionMs
  private val localAccumulatedDispersionPerMs =
    Config.gclockLocalAccumulatedDispersionPerMs

  private val state = AtomicReference[Option[State]](None)
  private val logger = Logger(getClass())

  private def withState[T](default: => T)(f: State => T): T = {
    state.get() match {
      case Some(s) => f(s)
      case None =>
        logger.warn("NTPGClock is not ready, using default value")
        default
    }
  }

  override def isReady(): Boolean = state.get().isDefined

  override def now(): Long = withState(System.currentTimeMillis()) { s =>
    val elapsedMs = s.elapsedMs()
    val serverTime = s.serverTime + elapsedMs
    serverTime.toLong
  }

  override def after(t: Long): Boolean = withState(false) { s =>
    val elapsedMs = s.elapsedMs()
    val localAccumulatedDispersion = elapsedMs * localAccumulatedDispersionPerMs
    val earliestBound =
      s.serverTime - s.serverTimeDispersion - localAccumulatedDispersion + elapsedMs
    t < earliestBound
  }

  override def before(t: Long): Boolean = withState(false) { s =>
    val elapsedMs = s.elapsedMs()
    val localAccumulatedDispersion = elapsedMs * localAccumulatedDispersionPerMs
    val latestBound =
      s.serverTime + s.serverTimeDispersion + localAccumulatedDispersion + elapsedMs
    t > latestBound
  }

  override protected def run(): Unit = {
    logger.info("Starting NTPGClock")
    while (true) {
      try {
        updateTime()
      } catch {
        case NonFatal(e) =>
          logger.error("Failed to update time", e)
      }
      Thread.sleep(10000)
    }
  }

  private def updateTime(): Unit = {
    val client = NTPUDPClient()
    val ntpServerAddr = InetAddress.getByName(ntpServer)

    val localStartTs = System.nanoTime()
    val message = client.getTime(ntpServerAddr).getMessage()
    val localEndTs = System.nanoTime()

    val stratum = message.getStratum()
    val rootDelay = message.getRootDelayInMillisDouble()
    val rootDispersion = message.getRootDispersionInMillisDouble()
    val localDelay = (localEndTs - localStartTs) / 1000000.0
    val totalDispersion =
      rootDelay + rootDispersion + localDelay + clockUncertaintyMs
    val serverTime = message.getTransmitTimeStamp().getTime().toDouble

    logger.info(
      "Calibrated NTP clock, stratum {}, delay {}ms, dispersion {}ms, local delay {}ms, total dispersion {}ms, server time {}",
      stratum,
      rootDelay,
      rootDispersion,
      localDelay,
      totalDispersion,
      serverTime.toLong
    )

    if (totalDispersion > maxDispersionMs) {
      throw new IllegalStateException(
        s"Total dispersion $totalDispersion exceeds maximum allowed $maxDispersionMs, not accepting"
      )
    }

    val oldSt = state.get()
    val newSt = State(
      lastUpdateNanoTime = localStartTs + (localEndTs - localStartTs) / 2,
      serverTime = serverTime,
      serverTimeDispersion = totalDispersion
    )

    oldSt match {
      case Some(old) =>
        val oldElapsedMs = old.elapsedMs()
        val oldAccumulatedDispersion =
          oldElapsedMs * localAccumulatedDispersionPerMs
        val oldTotalDispersion =
          old.serverTimeDispersion + oldAccumulatedDispersion

        if (newSt.serverTimeDispersion > oldTotalDispersion) {
          logger.warn(
            "New time is less accurate than old time, ignoring (old total dispersion = {}, new dispersion = {})",
            oldTotalDispersion,
            newSt.serverTimeDispersion
          )
        } else {
          state.set(Some(newSt))
        }
      case None =>
        state.set(Some(newSt))
    }

  }
}

private case class State(
    lastUpdateNanoTime: Long,
    serverTime: Double,
    serverTimeDispersion: Double
) {
  def elapsedMs(): Double = {
    val now = System.nanoTime()
    val elapsed = now - lastUpdateNanoTime
    val elapsedMs = elapsed / 1000000.0
    elapsedMs
  }
}
