FROM python:3.8-slim

RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements first for caching
COPY ./model/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy ALL application files at once
COPY . .

# Set Python path to include /app and subdirectories
#ENV PYTHONPATH="/app:${PYTHONPATH}"
ENV PYTHONPATH=/app

#CMD ["python", "/app/model/classification.py"]