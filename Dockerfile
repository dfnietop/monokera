#Dockerfile
FROM apache/airflow:2.10.3-python3.9
# Install additional dependencies
USER root
COPY requirements.txt ./requirements.txt
USER airflow
# Set up additional Python dependencies
RUN pip install --no-cache-dir --user -r ./requirements.txt
