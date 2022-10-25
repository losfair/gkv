package io.su3.gkv.mesh

@main def hello: Unit =
  val th = Thread.startVirtualThread(new Runnable {
    override def run(): Unit = {
      println("Hello, world!")
    }
  })

  th.join()
  println("end")
