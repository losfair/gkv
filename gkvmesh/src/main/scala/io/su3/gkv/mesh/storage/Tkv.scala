package io.su3.gkv.mesh.storage

trait Tkv {
  def beginTransaction(): TkvTxn = ???
  def transact[T](f: TkvTxn => T): T = {
    var txn = beginTransaction()

    try {
      while (true) {
        try {
          val ret = f(txn)
          txn.commit()
          return ret
        } catch {
          case e: Exception =>
            txn = txn.handleError(e)
        }
      }
    } finally {
      txn.close()
    }

    ???
  }
}

trait TkvTxn {
  def handleError(e: Exception): TkvTxn = throw e
  def get(key: Array[Byte]): Option[Array[Byte]]
  def put(key: Array[Byte], value: Array[Byte]): Unit
  def delete(key: Array[Byte]): Unit
  def commit(): Unit
  def close(): Unit
}
