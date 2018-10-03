package net.santiment.util

import com.typesafe.scalalogging.LazyLogging

class Migrator(migrations:Array[Migration], next:Store[Int], nextToDestroy:Store[Int])
extends LazyLogging {

  def up(): Unit = {
    val nxt = next.read.getOrElse(0)
    for(cur <- nxt until migrations.length) {
      migrations(cur).up()
      logger.info(s"Applied migration ${cur}: ${migrations(cur).name} ")
      next.write(Some(cur+1))
    }
  }

  def cleanUntil(last:Int): Unit = {
    val nxtd = nextToDestroy.read.getOrElse(0)
    val nxt = next.read.getOrElse(0)
    for( cur <- nxt until Math.min(nxt,last)) {
      migrations(cur).clean()
      logger.info(s"Cleaned migration $cur: ${migrations(cur).name}")
      nextToDestroy.write(Some(cur+1))
    }
  }

  def reset():Unit = {
    cleanUntil(next.read.getOrElse(0))
    next.delete()
    nextToDestroy.delete()
    logger.info("Migrations reset")
  }
}
