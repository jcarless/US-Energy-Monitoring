docker build -t jcarless/real-time-traffic:latest -t jcarless/real-time-traffic:$SHA .
docker push jcarless/real-time-traffic:latest
docker push jcarless/real-time-traffic:$SHA
helm secrets install rtt --values ./helm/secrets.yaml ./helm/.