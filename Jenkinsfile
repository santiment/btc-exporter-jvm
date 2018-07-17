podTemplate(label: 'btc-exporter-jvm-builder', containers: [
  containerTemplate(name: '8-jdk', image: 'openjdk:8-jdk', ttyEnabled: true, command: 'cat', envVars: [
    envVar(key: 'DOCKER_HOST', value: 'tcp://docker-host-docker-host:2375')
  ])
]) {
  node('btc-exporter-jvm-builder') {
    stage('Run Tests') {
      container('docker') {
        def scmVars = checkout scm

        sh "docker build -t btc-exporter-jvm:${scmVars.GIT_COMMIT} --build-arg UID=1000 -f Dockerfile-test ."
        //Run unit tests
        sh "docker run --rm -t btc-exporter-jvm:${scmVars.GIT_COMMIT} sbt test"

        //TODO: Run integration tests

        if (env.BRANCH_NAME == "master") {
          withCredentials([
            string(
              credentialsId: 'aws_account_id',
              variable: 'aws_account_id'
            )
          ]) {
            def awsRegistry = "${env.aws_account_id}.dkr.ecr.eu-central-1.amazonaws.com"
            docker.withRegistry("https://${awsRegistry}", "ecr:eu-central-1:ecr-credentials") {
              sh "docker build -t ${awsRegistry}/btc-exporter-jvm:${env.BRANCH_NAME} -t ${awsRegistry}/btc-exporter-jvm:${scmVars.GIT_COMMIT} ."
              sh "docker push ${awsRegistry}/btc-exporter-jvm:${env.BRANCH_NAME}"
              sh "docker push ${awsRegistry}/btc-exporter-jvm:${scmVars.GIT_COMMIT}"
            }
          }
        }

        // assumes you have the sbt plugin installed and created an sbt installation named 'sbt-default'

      }
    }
  }
}
