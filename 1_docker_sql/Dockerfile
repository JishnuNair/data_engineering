FROM python:3.9

RUN apt-get install wget
RUN pip install pandas sqlalchemy pg8000

WORKDIR /app
COPY src/ingest_data.py ingest_data.py 

ENTRYPOINT [ "python", "ingest_data.py" ]