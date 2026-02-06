FROM python:3.9-slim

WORKDIR /app

COPY services/workload-generator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/workload-generator/src/ .

CMD ["python", "-u", "main.py"]
