FROM python:2.7

MAINTAINER cole.duclos

RUN apt-get update

RUN pip install boto3 geohash 

COPY *.py ./

ENTRYPOINT python run.py
