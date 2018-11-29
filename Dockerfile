FROM python:3.7-alpine

COPY . /app
WORKDIR /app

RUN apk add  --no-cache ffmpeg

RUN pip install -r requirements.txt

ADD https://raw.githubusercontent.com/eficode/wait-for/master/wait-for /app
RUN chmod +x ./wait-for

RUN chmod +x ./main.py

CMD ["./main.py"]