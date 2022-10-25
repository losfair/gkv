package io.su3.gkv.mesh.storage

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Transaction;

class FdbTkv() extends Tkv {
  val fdbApi: FDB = FDB.selectAPIVersion(630)
  val db = fdbApi.open()

  override def beginTransaction(): TkvTxn = {
    val txn = db.createTransaction()
    new FdbTkvTxn(txn)
  }
}

private[storage] class FdbTkvTxn(val fdbTxn: Transaction) extends TkvTxn {
  override def handleError(e: Exception): TkvTxn = FdbTkvTxn(
    fdbTxn.onError(e).join()
  )

  override def get(key: Array[Byte]): Option[Array[Byte]] = {
    Option(fdbTxn.get(key).join())
  }

  override def put(key: Array[Byte], value: Array[Byte]): Unit = {
    fdbTxn.set(key, value)
  }

  override def delete(key: Array[Byte]): Unit = {
    fdbTxn.clear(key)
  }

  override def commit(): Unit = {
    fdbTxn.commit().join()
  }

  override def close(): Unit = {
    fdbTxn.close()
  }
}
