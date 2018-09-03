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

  private def topicMigration(topic:String, numPartitions:Int, replicationFactor:Short): Migration =
    Migration(
      name = s"Create topic $topic",
      up = ()=> {
        val  config = Map[String,String](
          ("compression.type", "lz4")
        )
        val t = new NewTopic(topic,numPartitions,replicationFactor)

        t.configs(config.asJava)
        kafka.createTopics(Seq(t).asJava)
      },

      clean = ()=>{
        kafka.deleteTopics(Seq(topic).asJava)
      })

  val migrations:Array[Migration] = Array(
    topicMigration("btc-transfers-1",1,3)
  )
}