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
                    // Build image từ Dockerfile trong thư mục dagster_vic
                    // Lưu ý: dir('dagster_vic') để vào đúng thư mục chứa Dockerfile
                    dir('dagster_vic') {
                        dockerImage = docker.build("${IMAGE_NAME}:${IMAGE_TAG}")
                    }
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
        
        stage('Deploy to EC2 (Optional)') {
            // Bước này có thể thêm sau để trigger Ansible deploy hoặc update service
            steps {
                echo 'Deployment step placeholder'
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

