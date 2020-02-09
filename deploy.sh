docker build -t jcarless/real-time-traffic:latest -t jcarless/real-time-traffic:$SHA .
docker push jcarless/real-time-traffic:latest
docker push jcarless/real-time-traffic:$SHA
openssl aes-256-cbc -K $encrypted_4bf0def5b715_key -iv $encrypted_4bf0def5b715_iv -in secrets.tar.enc -out secrets.tar -d
tar -xvf secrets.tar
curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3
chmod 700 get_helm.sh
./get_helm.sh
helm init
helm plugin install https://github.com/futuresimple/helm-secrets
GPG_TTY=$(tty)
export GPG_TTY
echo "===import==="
echo $gpgpw | gpg --passphrase-fd 0 --import ./key.asc
echo "===--list-keys==="
gpg --list-keys
echo "===--list-secret-keys==="
gpg --list-secret-keys
echo "===ls==="
cat /home/travis/.gnupg/gpg.conf
echo "===helm secrets==="
helm secrets install rtt --values ./helm/secrets.yaml ./helm/.