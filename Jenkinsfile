podTemplate(label: 'btc-exporter-jvm-builder', containers: [
  containerTemplate(name: 'docker-compose', image: 'docker/compose:1.22.0', ttyEnabled: true, command: 'cat', envVars: [
    envVar(key: 'DOCKER_HOST', value: 'tcp://docker-host-docker-host:2375')
  ])
]) {
  node('btc-exporter-jvm-builder') {
    
    def scmVars = checkout scm 

    container('docker-compose') {
      stage('Build') {
        sh "docker build --target builder -t localhost/btc-exporter-jvm-builder:${scmVars.GIT_COMMIT} ."
      }

      stage('Test') {
        withCredentials([
          string(
            credentialsId: 'stage-bitcoind-user',
            variable: 'BITCOIND_USER'
          ),
          string(
            credentialsId: 'stage-bitcoind-password',
            variable: 'BITCOIND_PASSWORD'
          )
        ]) {
          try {
            sh "docker-compose -f compose-test build test"
            sh "docker-compose -f compose-test.yml run test"
          } finally {
            sh "docker-compose -f compose-test.yml down -v"
          }
        }
      }

      if (env.BRANCH_NAME == "master") {
        stage('Publish') {
          withCredentials([
            string(
              credentialsId: 'aws_account_id',
              variable: 'aws_account_id'
            )
          ]) {
            def awsRegistry = "${env.aws_account_id}.dkr.ecr.eu-central-1.amazonaws.com"
            docker.withRegistry("https://${awsRegistry}", "ecr:eu-central-1:ecr-credentials") {
              sh "docker build --target rawexporter -t ${awsRegistry}/btc-exporter-jvm:raw-exporter-${env.BRANCH_NAME} -t ${awsRegistry}/btc-exporter-jvm:raw-exporter-${scmVars.GIT_COMMIT} ."
              sh "docker push ${awsRegistry}/btc-exporter-jvm:raw-exporter-${env.BRANCH_NAME}"
              sh "docker push ${awsRegistry}/btc-exporter-jvm:raw-exporter-${scmVars.GIT_COMMIT}"
            }
          }
        }
      }
    }
  }
}
