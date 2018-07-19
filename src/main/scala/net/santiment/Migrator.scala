package net.santiment

import java.util.Properties
import scala.collection.JavaConverters._

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}

case class Migration(name:String, up:()=>Unit, clean:()=>Unit)
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

class Migrations(kafka: AdminClient) {
  val migrations:Array[Migration] = Array(
    Migration(name = "Create topic btc-transfers-1",
      up = ()=> {
        val topic = new NewTopic("btc-tranfers-1",1,3)
        kafka.createTopics(Seq(topic).asJava)
      },

      clean = ()=>{
        kafka.deleteTopics(Seq("btc-transfers-1").asJava)
      })
  )
}