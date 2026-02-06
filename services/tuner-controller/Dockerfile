FROM python:3.9-slim

WORKDIR /app

COPY services/tuner-controller/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/tuner-controller/src/ .
COPY configs /app/configs
RUN mkdir -p /app/artifacts

CMD ["python", "-u", "main.py"]
