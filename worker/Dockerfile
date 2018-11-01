FROM python:3.7-alpine

RUN adduser -D cidev
WORKDIR /home/cidev

COPY worker/requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY worker/src src
ADD common/src src/
COPY worker/boot.sh ./
RUN chmod +x boot.sh

RUN chown -R cidev:cidev ./
USER cidev

ENTRYPOINT ["./boot.sh"]