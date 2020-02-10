docker build -t jcarless/real-time-traffic:latest -t jcarless/real-time-traffic:$SHA .
docker push jcarless/real-time-traffic:latest
docker push jcarless/real-time-traffic:$SHA
openssl aes-256-cbc -K $encrypted_4bf0def5b715_key -iv $encrypted_4bf0def5b715_iv -in secrets.tar.enc -out secrets.tar -d
tar -xvf secrets.tar
curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3
chmod 700 get_helm.sh
./get_helm.sh
helm init
helm dependency update ./helm
helm install rtt --values ./helm/secrets.yaml ./helm/.