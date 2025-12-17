# Dockerfile for ETL Development and Testing
FROM python:3.11-slim

# Install Java for PySpark
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jdk procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy and install requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Set environment variables
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Default command
CMD ["bash"]
