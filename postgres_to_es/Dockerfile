FROM python:3.8

WORKDIR /opt/etl

COPY ./src .
RUN pip install -r requirements.txt

CMD ["python", "main.py"]