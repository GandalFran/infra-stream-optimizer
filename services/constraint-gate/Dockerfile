FROM python:3.9-slim

WORKDIR /app

COPY services/constraint-gate/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/constraint-gate/src/ .

ENV PYTHONUNBUFFERED=1

CMD ["gunicorn", "-b", "0.0.0.0:8000", "--access-logfile", "-", "--error-logfile", "-", "main:app"]
