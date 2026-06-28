pipeline {
    agent any

    environment {
        PROJECT_NAME = "${env.JOB_NAME}"
        IMAGE_NAME = "${env.REGISTRY_URL}/${PROJECT_NAME}:${env.BUILD_NUMBER}"
    }

    stages {
        stage('构建镜像') {
            steps {
                script {
                    sh '''
                        echo '============================== 构建镜像 =============================='
                        cp /var/jenkins_home/settings.xml ./settings.xml
                        cp ../apache-maven-3.6.3-bin.tar.gz .
                        docker build --network appnet -t ${IMAGE_NAME} -f ../Dockerfile .
                    '''
                }
            }
        }
        stage('上传镜像') {
            steps {
                script {
                    sh '''
                        echo '============================== 上传镜像 =============================='
                        docker push ${IMAGE_NAME}
                    '''
                }
            }
        }
        stage('运行镜像') {
            steps {
                script {
                    sh '''
                        echo '============================== 运行镜像 =============================='
                        if [ -n \"\$(docker ps -q -f name=${PROJECT_NAME})" ]; then
                            docker stop ${PROJECT_NAME}
                        fi
                        if [ -n \"\$(docker ps -aq -f name=${PROJECT_NAME})" ]; then
                            docker rm ${PROJECT_NAME}
                        fi
                        docker pull ${IMAGE_NAME}
                        docker run -d --name ${PROJECT_NAME} -e JAVA_OPTS="-XX:-UseContainerSupport" ${IMAGE_NAME}
                        sleep 10
                        docker logs ${PROJECT_NAME}
                    '''
                }
            }
        }
    }
}