kubectl create namespace etl
kubectl apply -f spark-rbac-v2.yaml
helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator
helm install my-release spark-operator/spark-operator --namespace spark-operator --create-namespace
kubectl apply -f deployment.yaml