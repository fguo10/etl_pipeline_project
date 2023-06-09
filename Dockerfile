FROM apache/airflow:2.6.1-python3.8

ADD . /app
WORKDIR /app
RUN pip install -r requirements.txt

CMD ["python", "app.py"]
EXPOSE 5000