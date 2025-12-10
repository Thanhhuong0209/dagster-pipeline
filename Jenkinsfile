pipeline {
    agent any

    environment {
        // Cấu hình Docker Hub Credential ID (đã tạo trong Jenkins)
        DOCKER_HUB_CREDENTIALS_ID = 'docker-hub-credentials' 
        // Tên image trên Docker Hub
        IMAGE_NAME = 'thanhhuong29/dagster-user-code' 
        IMAGE_TAG = 'latest'
    }

    stages {
        stage('Checkout') {
            steps {
                // Checkout code từ Git
                checkout scm
            }
        }

        stage('Build Docker Image') {
            steps {
                script {
                    // Build image từ Dockerfile tại thư mục hiện tại (root của repo)
                    // Đã bỏ dir('dagster_vic') vì trên GitHub Dockerfile nằm ngay root
                    dockerImage = docker.build("${IMAGE_NAME}:${IMAGE_TAG}")
                }
            }
        }

        stage('Push to Docker Hub') {
            steps {
                script {
                    // Đăng nhập và Push image lên Docker Hub
                    docker.withRegistry('', DOCKER_HUB_CREDENTIALS_ID) {
                        dockerImage.push()
                        dockerImage.push(IMAGE_TAG)
                    }
                }
            }
        }
        
        stage('Deploy to EC2') {
            steps {
                script {
                    // Chạy lệnh docker compose để deploy
                    // Lưu ý: File docker-compose.yml đã nằm sẵn trong workspace do checkout từ git
                    sh '''
                        docker compose down
                        docker compose pull
                        docker compose up -d
                    '''
                }
            }
        }
    }

    post {
        success {
            echo 'Build and Push successful!'
        }
        failure {
            echo 'Build failed.'
        }
    }
}

