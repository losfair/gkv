package io.su3.gkv.mesh.storage

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.tuple.Tuple;
import com.typesafe.scalalogging.Logger
import scala.collection.mutable.ArrayBuffer
import java.nio.ByteBuffer
import java.util.concurrent.Future
import java.util.concurrent.CompletableFuture
import io.su3.gkv.mesh.config.Config
import scala.util.control.NonFatal.apply
import scala.util.control.NonFatal
import java.util.concurrent.ExecutionException
import java.util.concurrent.CompletionException
import scala.util.Try
import scala.util.Success
import scala.util.Failure

object TkvKeyspace {
  val distributedLockPrefix = "distributedLock"
  val merkleTreeHashBufferPrefix = "mthb"
  val merkleTreeStructurePrefix = "mts"
  val dataPrefix = "data"

  def constructMerkleTreeStructureKey(
      prefix: Seq[Byte]
  ): Array[Byte] = {
    Tuple
      .from(
        merkleTreeStructurePrefix,
        prefix.length
      )
      .pack() ++ prefix
  }
}

class Tkv(val prefix: Array[Byte]) extends AutoCloseable {
  val fdbApi: FDB = Tkv.forceInitFdb()
  val db = Tkv.forceOpenFdb(fdbApi)

  def beginTransaction(): TkvTxn = {
    val txn = db.createTransaction()

    new TkvTxn(this, txn)
  }

  def transact[T](f: TkvTxn => T): T = {
    Try(db.run(txn => {
      val tkvTxn = new TkvTxn(this, txn)
      // XXX: https://github.com/apple/foundationdb/pull/8623
      Try(f(tkvTxn)) match {
        case Success(x) => x
        case Failure(err: ExecutionException) if err.getCause() != null =>
          throw err.getCause()
        case Failure(err) => throw err
      }
    })) match {
      case Success(x) => x
      case Failure(err: CompletionException) if err.getCause() != null =>
        throw err.getCause()
      case Failure(err) => throw err
    }
  }

  override def close(): Unit = {
    db.close()
  }
}

object Tkv {
  val logger = Logger(getClass())

  private def forceInitFdb(): FDB = {
    val apiVersion = 630
    try {
      FDB.selectAPIVersion(apiVersion)
    } catch {
      case e: FDBException =>
        if (e.getCode() == 2201) {
          // "API version may be set only once"
          logger.warn(
            "FDB API version already set, patching"
          )

          val constructor = classOf[FDB].getDeclaredConstructor(classOf[Int])
          constructor.setAccessible(true)
          val api = constructor.newInstance(apiVersion)

          val singleton = classOf[FDB].getDeclaredField("singleton")
          singleton.setAccessible(true)
          singleton.set(null, api)

          api
        } else {
          throw e
        }
    }
  }

  private def forceOpenFdb(api: FDB): Database = {
    try {
      val options = api.options()

      if (Config.fdbBuggify) {
        options.setClientBuggifyEnable()
        options.setClientBuggifySectionActivatedProbability(100)
        logger.warn("FDB buggify enabled")
      }

      api.startNetwork()
    } catch {
      case e: FDBException =>
        if (e.getCode() == 2009) {
          // "Network can be configured only once"
          logger.warn(
            "FDB network already configured, patching"
          )
          val field = classOf[FDB].getDeclaredField("netStarted")
          field.setAccessible(true)
          field.set(api, true)
        } else {
          throw e
        }
    }

    api.open()
  }
}

sealed trait TkvTxnReadMode

object TkvTxnReadMode {
  case object Snapshot extends TkvTxnReadMode
  case object Serializable extends TkvTxnReadMode
}

object TkvTxn {
  val metadataVersionKey = Array(0xff.toByte) ++ "/metadataVersion".getBytes()
}

class TkvTxn(
    val tkv: Tkv,
    private val fdbTxn: Transaction
) extends AutoCloseable {
  override def close(): Unit = {
    fdbTxn.close()
  }

  def setBatchPriority(): Unit = {
    fdbTxn.options().setPriorityBatch()
  }

  def asyncGet(
      key: Array[Byte],
      mode: TkvTxnReadMode = TkvTxnReadMode.Serializable
  ): CompletableFuture[Option[Array[Byte]]] = {
    (mode match {
      case TkvTxnReadMode.Serializable => fdbTxn.get(tkv.prefix ++ key)
      case TkvTxnReadMode.Snapshot => fdbTxn.snapshot().get(tkv.prefix ++ key)
    }).thenApply(Option(_))
  }

  def get(
      key: Array[Byte],
      mode: TkvTxnReadMode = TkvTxnReadMode.Serializable
  ): Option[Array[Byte]] = {
    asyncGet(key, mode).get()
  }

  def put(key: Array[Byte], value: Array[Byte]): Unit = {
    fdbTxn.set(tkv.prefix ++ key, value)
  }

  def delete(key: Array[Byte]): Unit = {
    fdbTxn.clear(tkv.prefix ++ key)
  }

  def compareAndDelete(key: Array[Byte], expectedValue: Array[Byte]): Unit = {
    fdbTxn.mutate(
      MutationType.COMPARE_AND_CLEAR,
      tkv.prefix ++ key,
      expectedValue
    )
  }

  def snapshotRange(
      begin: Array[Byte],
      end: Array[Byte],
      limit: Int
  ): Seq[(Array[Byte], Array[Byte])] = {
    val range =
      fdbTxn.snapshot().getRange(tkv.prefix ++ begin, tkv.prefix ++ end, limit)
    val iterator = range.iterator()
    val ret = ArrayBuffer[(Array[Byte], Array[Byte])]()
    while (iterator.hasNext()) {
      val kv = iterator.next()
      ret += ((
        kv.getKey().slice(tkv.prefix.length, kv.getKey().length),
        kv.getValue()
      ))
    }

    ret.toSeq
  }

  def commit(): Unit = {
    fdbTxn.commit().join()
  }

  def getMetadataVersion(): Long = {
    val value = fdbTxn.snapshot().get(TkvTxn.metadataVersionKey).join()
    if (value == null) {
      return 0
    }
    if (value.length < 8) {
      return 0
    }

    ByteBuffer.allocate(8).put(value).getLong(0)
  }

  def updateMetadataVersion(): Unit = {
    val param = Array.fill(14)(0.toByte)
    fdbTxn.mutate(
      MutationType.SET_VERSIONSTAMPED_VALUE,
      TkvTxn.metadataVersionKey,
      param
    )
  }
}
