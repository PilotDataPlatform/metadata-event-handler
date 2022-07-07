pipeline {
    agent { label 'small' }
    environment {
      imagename = "ghcr.io/pilotdataplatform/metadata-event-handler"
      commit = sh(returnStdout: true, script: 'git describe --always').trim()
      registryCredential = 'pilot-ghcr'
      dockerImage = ''
    }

    stages {

    stage('DEV Git clone') {
        when { branch 'develop' }
        steps {
            git branch: 'develop',
                url: 'https://github.com/PilotDataPlatform/metadata-event-handler.git',
                credentialsId: 'pilot-gh'
        }
    }

    // stage('DEV Run unit tests') {
    //     when { branch 'develop' }
    //     steps {
    //         withCredentials([
    //             usernamePassword(credentialsId: 'indoc-ssh', usernameVariable: 'SUDO_USERNAME', passwordVariable: 'SUDO_PASSWORD'),
    //             string(credentialsId:'VAULT_TOKEN', variable: 'VAULT_TOKEN'),
    //             string(credentialsId:'VAULT_URL', variable: 'VAULT_URL'),
    //             file(credentialsId:'VAULT_CRT', variable: 'VAULT_CRT')
    //         ]) {
    //             sh """
    //             export OPSDB_UTILILY_USERNAME=postgres
    //             export OPSDB_UTILILY_PASSWORD=postgres
    //             export OPSDB_UTILILY_HOST=db
    //             export OPSDB_UTILILY_PORT=5432
    //             export OPSDB_UTILILY_NAME=metadata-event-handler
    //             [ ! -f ${env.WORKSPACE}/.env ] && touch ${env.WORKSPACE}/.env
    //             [ -d ${env.WORKSPACE}/local_config/pgadmin/sessions ] && sudo chmod 777 -R -f ${env.WORKSPACE}/local_config/pgadmin/sessions
    //             sudo chmod 777 -R -f ${env.WORKSPACE}/local_config/pgadmin/sessions
    //             docker build -t web .
    //             docker-compose -f docker-compose.yaml down -v
    //             docker-compose up -d
    //             sleep 10s
    //             docker-compose exec -T web /bin/bash
    //             pwd
    //             hostname
    //             docker-compose exec -T web pip install --user poetry==1.1.12
    //             docker-compose exec -T web poetry config virtualenvs.in-project false
    //             docker-compose exec -T web poetry install --no-root --no-interaction
    //             docker-compose exec -T web poetry run pytest --verbose -c tests/pytest.ini
    //             docker-compose -f docker-compose.yaml down -v
    //             """
    //         }
    //     }
    // }

    stage('DEV Build and push image') {
      when {branch "develop"}
      steps {
        script {
          docker.withRegistry('https://ghcr.io', registryCredential) {
              customImage = docker.build("$imagename:$commit")
              customImage.push()
          }
        }
      }
    }

    stage('DEV Remove image') {
      when {branch "develop"}
      steps{
        sh "docker rmi $imagename:$commit"
      }
    }

    stage('DEV Deploy') {
      when {branch "develop"}
      steps{
        build(job: "/VRE-IaC/UpdateAppVersion", parameters: [
          [$class: 'StringParameterValue', name: 'TF_TARGET_ENV', value: 'dev' ],
          [$class: 'StringParameterValue', name: 'TARGET_RELEASE', value: 'metadata-event-handler' ],
          [$class: 'StringParameterValue', name: 'NEW_APP_VERSION', value: "$commit" ]
        ])
      }
    }

  }
  post {
      failure {
        slackSend color: '#FF0000', message: "Build Failed! - ${env.JOB_NAME} $commit  (<${env.BUILD_URL}|Open>)", channel: 'jenkins-dev-staging-monitor'
      }
  }

}
