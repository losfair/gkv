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
    db.run(txn => {
      val tkvTxn = new TkvTxn(this, txn)
      try {
        f(tkvTxn)
      } catch {
        case e: Exception =>
          // XXX: https://github.com/apple/foundationdb/pull/8623
          if (
            (e.isInstanceOf[CompletionException] || e
              .isInstanceOf[ExecutionException]) && e
              .getCause() != null
          ) {
            Tkv.logger.debug(
              "txn error",
              e.getCause()
            )
            throw e.getCause()
          } else {
            throw e
          }
      }
    })
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

  def asyncGet(key: Array[Byte]): CompletableFuture[Option[Array[Byte]]] = {
    fdbTxn.get(tkv.prefix ++ key).thenApply(Option(_))
  }

  def get(key: Array[Byte]): Option[Array[Byte]] = {
    asyncGet(key).get()
  }

  def put(key: Array[Byte], value: Array[Byte]): Unit = {
    fdbTxn.set(tkv.prefix ++ key, value)
  }

  def delete(key: Array[Byte]): Unit = {
    fdbTxn.clear(tkv.prefix ++ key)
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

  def addReadConflictKeys(keys: Seq[Array[Byte]]): Unit = {
    keys.foreach(key => {
      fdbTxn.addReadConflictKey(tkv.prefix ++ key)
    })
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
