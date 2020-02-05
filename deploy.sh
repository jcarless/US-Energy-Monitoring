docker build -t jcarless/real-time-traffic:latest -t jcarless/real-time-traffic:$SHA .
docker push jcarless/real-time-traffic:latest
docker push jcarless/real-time-traffic:$SHA
gpg --import helm-values-secret.gpg
helm secrets install rtt --values ./helm/secrets.yaml ./helm/.