package net.santiment

trait Periodic {
  val period: Int = 10000

  private var startTs = System.currentTimeMillis()

  def occasionally(f: => Unit):Unit = {
    var test = System.currentTimeMillis()
    if (test - startTs >= period) {
      f
      startTs = test

    }
  }
}
