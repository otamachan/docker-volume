FROM alpine:3.6
MAINTAINER "Tamaki Nishino <otamachan@gmail.com>"
RUN apk add --update python py-pip \
    && rm -rf /var/cache/apk/*
RUN pip install boto3
ADD volume.py /volume.py
EXPOSE 8000
ENTRYPOINT ["python", "/volume.py"]

