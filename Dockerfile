FROM python:3.7-alpine

RUN adduser -D cidev
WORKDIR /home/cidev

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
RUN pip install gunicorn

COPY src src
COPY boot.sh ./
RUN chmod +x boot.sh

ENV FLASK_APP src/main.py

RUN chown -R cidev:cidev ./
USER cidev

EXPOSE 8080
ENTRYPOINT ["./boot.sh"]
