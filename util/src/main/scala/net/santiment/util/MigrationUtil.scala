package net.santiment.util

import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import collection.JavaConverters._

object MigrationUtil {

  def topicMigration(kafka: AdminClient, topic:String, numPartitions:Int, replicationFactor:Short): Migration =
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

}
