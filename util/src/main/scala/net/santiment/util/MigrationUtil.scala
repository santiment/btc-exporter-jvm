package net.santiment.util

import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}

import collection.JavaConverters._

object MigrationUtil extends LazyLogging {

  def getTopics(kafka:AdminClient): java.util.Set[String] = {
    kafka.listTopics().names().get()
  }

  def topicMigration(kafka: AdminClient, topic:String, numPartitions:Int, replicationFactor:Short): Migration =
    Migration(
      name = s"Create topic $topic",
      up = ()=> {
        if (getTopics(kafka).contains(topic)) {
          logger.warn(s"Topic $topic already exists. Skipping creation")
        } else {

          val config = Map[String, String](
            ("compression.type", "lz4"),
            //upper limit for fetch size
            ("max.message.bytes", "52428800")
          )
          val t = new NewTopic(topic, numPartitions, replicationFactor)

          t.configs(config.asJava)
          kafka.createTopics(Seq(t).asJava)
        }
      },

      clean = ()=>{
        if (getTopics(kafka).contains(topic)) {
          kafka.deleteTopics(Seq(topic).asJava).all().get(30, TimeUnit.SECONDS)
        } else {
          logger.warn(s"Topic $topic does not exist. Skipping deletion")
        }
      })

  def compactTopicMigration(kafka: AdminClient, topic:String, numPartitions:Int, replicationFactor:Short): Migration =
    compactTopicMigration(kafka, Seq(topic), numPartitions, replicationFactor)

  def compactTopicMigration(kafka: AdminClient, topics:Seq[String], numPartitions:Int, replicationFactor:Short): Migration =
    Migration(
      name = s"Create topics ${topics.mkString(",")}",
      up = ()=> {
        val oldTopics = getTopics(kafka)

        for ( topic <- topics if oldTopics.contains(topic) ) {
          logger.warn(s"Topic $topic already exists. Skipping creation")
        }

        val preparedTopics:Seq[NewTopic] = for (topic <- topics if !oldTopics.contains(topic) ) yield {

            val config = Map[String, String](
              ("compression.type", "lz4"),
              ("cleanup.policy", "compact"),
              //upper limit for fetch size
              ("max.message.bytes", "52428800")
            )

            val t = new NewTopic(topic, numPartitions, replicationFactor)
            t.configs(config.asJava)
            t
          }

        kafka.createTopics(preparedTopics.asJava).all().get(30, TimeUnit.SECONDS)

      },

      clean = ()=>{
        val oldTopics = getTopics(kafka)

        for ( topic <- topics if !oldTopics.contains(topic)) {
          logger.warn(s"Topic $topic doesn't exist. Skipping deletion")
        }

        val preparedTopics: Seq[String] = topics.filter(oldTopics.contains(_))
        kafka.deleteTopics(preparedTopics.asJava).all().get(30, TimeUnit.SECONDS)
      })


  def cleanMigration(migration:Migration):Migration =
    Migration(
      name = s"rollback-${migration.name}",
      up = migration.clean,
      clean = ()=>{}
    )

}
