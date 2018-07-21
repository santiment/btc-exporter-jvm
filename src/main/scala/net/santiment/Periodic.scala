package net.santiment

trait Periodic[State] {
  val period: Int = 10000

  var state:State = _
  private var startTs = System.currentTimeMillis()

  def occasionally(f: State => State):Unit = {
    var test = System.currentTimeMillis()
    if (test - startTs >= period) {
      state = f(state)
      startTs = test

    }
  }
}
