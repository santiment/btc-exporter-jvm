podTemplate(label: 'btc-exporter-jvm-builder', containers: [
  containerTemplate(name: '8-jdk', image: 'openjdk:8-jdk', ttyEnabled: true, command: 'cat', envVars: [
    envVar(key: 'DOCKER_HOST', value: 'tcp://docker-host-docker-host:2375')
  ])
]) {
  node('etherbi-flink-builder') {
    stage('Build') {
      container('8-jdk') {
        def scmVars = checkout scm

        // assumes you have the sbt plugin installed and created an sbt installation named 'sbt-default'
        sh "${tool name: 'sbt-default', type: 'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'}/bin/sbt docker:publishLocal"

      }
    }
  }
}
