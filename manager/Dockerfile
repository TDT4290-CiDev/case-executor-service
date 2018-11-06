FROM python:3.7-alpine

RUN adduser -D cidev
WORKDIR /home/cidev

COPY manager/requirements.txt requirements.txt
RUN pip install -r requirements.txt
RUN pip install gunicorn

COPY manager/src src
ADD common/src src/
COPY manager/boot.sh ./
RUN chmod +x boot.sh

ENV FLASK_APP src/main.py

RUN chown -R cidev:cidev ./
USER cidev

EXPOSE 8080
ENTRYPOINT ["./boot.sh"]
