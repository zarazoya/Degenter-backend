pipeline {
  agent any

  environment {
    DOCKER_USER        = ''                        // optional override; defaults to creds
    IMAGE_NAMESPACE    = 'beencolabs'              // Docker Hub org/user
    IMAGE_REPO         = 'degenter-indexer'        // repo to create/push
    IMAGE_NAME         = "${IMAGE_NAMESPACE}/${IMAGE_REPO}"
    DOCKER_CREDENTIALS = 'dockerhub-credentials'
    GIT_CREDENTIALS    = 'github-token'
    DEPLOY_USER        = 'root'
    DEPLOY_HOST        = '159.223.28.88'
    DEPLOY_PATH        = '/opt/degenter'
  }

  stages {
    stage('Checkout') {
      steps {
        git branch: 'main',
            credentialsId: env.GIT_CREDENTIALS,
            url: 'https://github.com/cryptocomicsdevs/degenter-indexer-postgresql.git'
      }
    }

    stage('Ensure Docker Hub Repo') {
      steps {
        withCredentials([usernamePassword(credentialsId: DOCKER_CREDENTIALS,
                                          usernameVariable: 'DH_USER',
                                          passwordVariable: 'DH_PASS')]) {
          sh '''
            DHU="${DOCKER_USER:-$DH_USER}"
            TOKEN=$(curl -s -X POST https://hub.docker.com/v2/users/login \
              -H "Content-Type: application/json" \
              -d "{\"username\": \"${DHU}\", \"password\": \"${DH_PASS}\"}" | jq -r .token)

            EXISTS=$(curl -s -o /dev/null -w "%{http_code}" \
              -H "Authorization: JWT $TOKEN" \
              https://hub.docker.com/v2/repositories/${IMAGE_NAMESPACE}/${IMAGE_REPO}/)

            if [ "$EXISTS" != "200" ]; then
              echo "Creating repo ${IMAGE_NAMESPACE}/${IMAGE_REPO}..."
              curl -s -X POST https://hub.docker.com/v2/repositories/ \
                -H "Content-Type: application/json" \
                -H "Authorization: JWT $TOKEN" \
                -d "{\"namespace\": \"${IMAGE_NAMESPACE}\", \"name\": \"${IMAGE_REPO}\", \"is_private\": false}"
            else
              echo "Repo ${IMAGE_NAMESPACE}/${IMAGE_REPO} already exists."
            fi
          '''
        }
      }
    }

    stage('Build Docker Image') {
      steps {
        sh "docker build -t ${IMAGE_NAME}:${BUILD_NUMBER} ."
      }
    }

    stage('Push to Docker Hub') {
      steps {
        withCredentials([usernamePassword(credentialsId: DOCKER_CREDENTIALS,
                                          usernameVariable: 'DH_USER',
                                          passwordVariable: 'DH_PASS')]) {
          sh """
            echo "$DH_PASS" | docker login -u "$DH_USER" --password-stdin
            docker push ${IMAGE_NAME}:${BUILD_NUMBER}
            docker tag  ${IMAGE_NAME}:${BUILD_NUMBER} ${IMAGE_NAME}:latest
            docker push ${IMAGE_NAME}:latest
            docker logout
          """
        }
      }
    }

    stage('Deploy to Server') {
      steps {
        sshagent(credentials: ['server-ssh-key']) {
          sh """
            ssh -o StrictHostKeyChecking=no ${DEPLOY_USER}@${DEPLOY_HOST} '
              set -e
              docker pull ${IMAGE_NAME}:latest
              cd ${DEPLOY_PATH}
              docker compose up -d
            '
          """
        }
      }
    }
  }

  post {
    always {
      sh 'docker image prune -f || true'
    }
  }
}
