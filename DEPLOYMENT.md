# Deployment Docs

## Prerequisites

- Phala Cloud Account
- Follow Prerequisites and Installation [here](https://docs.phala.network/phala-cloud/phala-cloud-user-guides/advanced-deployment-options/start-from-cloud-cli)

## Build OTC Server Dockerfile

```bash
GIT_COMMIT=$(git rev-parse --short HEAD)
GIT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
VERSION_TAG="${GIT_BRANCH}-${GIT_COMMIT}"

docker build -t riftresearch/otc-server:${VERSION_TAG} -f Dockerfile.otc
docker tag riftresearch/otc-server:${VERSION_TAG} riftresearch/otc-server:latest

docker push riftresearch/otc-server:${VERSION_TAG}
docker push riftresearch/otc-server:latest
```
