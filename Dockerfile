FROM python:latest

WORKDIR /py-orbit-db-http-client
COPY . .
RUN pip install .