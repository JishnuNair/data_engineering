FROM python:3

WORKDIR /usr/src/app

COPY requirements.txt ./

ENV GOOGLE_APPLICATION_CREDENTIALS=/usr/src/app/credentials/dtc-de-course-339115-73c640745202.json

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python", "src/imdb_gcs_pipeline.py"]
