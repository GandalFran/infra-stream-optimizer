FROM python:3.10-slim

WORKDIR /app

COPY services/canary-deployer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Helm
RUN apt-get update && apt-get install -y curl && \
    curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash

COPY services/canary-deployer/src/ .
COPY k8s/ /app/k8s/

CMD ["python", "main.py"]
