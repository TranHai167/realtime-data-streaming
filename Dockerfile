
FROM apache/airflow:2.4.2

# USER airflow

# Install any additional system dependencies if needed

# Install additional dependencies, including kafka
COPY requirements.txt /requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt 