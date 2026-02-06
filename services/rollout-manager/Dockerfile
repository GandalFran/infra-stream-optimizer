FROM python:3.10-slim

WORKDIR /app

RUN pip install --no-cache-dir flask requests kubernetes

COPY services/rollout-manager/src/ .

EXPOSE 8000

CMD ["python", "main.py"]
