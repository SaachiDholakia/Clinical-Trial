FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
COPY main.py .
COPY validation ./validation

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python"]
CMD ["-u", "main.py"]