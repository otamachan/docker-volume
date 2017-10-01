# -*- coding: utf-8 -*-
import io
import logging

import boto3

DEFAULT_UPLOAD_PART_SIZE = 50 * 1024**2

logger = logging.getLogger(__name__)


def open(bucket_id, key_id, mode, **kwargs):
    if mode in ("rb"):
        fileobj = ReadFile(bucket_id, key_id, **kwargs)
    elif mode in ("wb"):
        fileobj = WriteFile(bucket_id, key_id, **kwargs)
    else:
        assert False
    return fileobj


class ReadFile(io.BufferedIOBase):
    def __init__(self, bucket, key, **kwargs):
        self.buffer_size = kwargs.get("buffer_size", io.DEFAULT_BUFFER_SIZE)
        session = boto3.Session()
        s3 = session.resource("s3")
        self.s3_obj = s3.Object(bucket, key)
        self.content_length = self.s3_obj.content_length
        self.current_pos = 0
        self.buf = b""
        self.eof = False

    def seek(self, offset, from_what=0):
        if from_what == 0:
            new_pos = offset
        elif from_what == 1:
            new_pos = self.current_pos + offset
        elif from_what == 2:
            new_pos = self.content_length + offset
        else:
            raise ValueError("Invalue seek")
        if new_pos < 0:
            new_pos = 0
        elif new_pos >= self.content_length:
            new_pos = self.content_length
        # reset
        self.current_pos = new_pos
        self.buf = b""
        self.eof = self.current_pos == self.content_length
        return self.current_pos

    def tell(self):
        return self.current_pos

    def read(self, size=-1):
        if size <= 0:
            # download all
            self.eof = True
            self.buf += self.download()
        else:
            while len(self.buf) < size and not self.eof:
                raw = self.download(size=self.buffer_size)
                if len(raw):
                    self.buf += raw
                else:
                    self.eof = True

        if self.eof:
            part = self.buf
            self.buf = b""
            self.current_pos = self.content_length
        else:
            part = self.buf[:size]
            self.buf = self.buf[size:]
            self.current_pos += size
        return part

    def download(self, size=-1):
        start = self.current_pos + len(self.buf)
        if start == self.content_length:
            return b""
        if size <= 0:
            rng = "bytes={0}-".format(start)
        else:
            rng = "bytes={0}-{1}".format(
                start,
                min(self.content_length, start + size))
        logger.debug("downloading ... %s", rng)
        body = self.s3_obj.get(Range=rng)["Body"].read()
        logger.debug("downloading done: %d", len(body))
        return body


class WriteFile(io.BufferedIOBase):
    def __init__(self, bucket, key, upload_part_size=DEFAULT_UPLOAD_PART_SIZE):
        session = boto3.Session()
        s3 = session.resource("s3")
        s3.create_bucket(Bucket=bucket)
        self.s3_obj = s3.Object(bucket, key)
        self.multipart_upload = self.s3_obj.initiate_multipart_upload()
        self.upload_part_size = upload_part_size
        self.buf = io.BytesIO()
        self.total_size = 0
        self.parts = []

    def close(self):
        if self.multipart_upload is None:
            return
        if self.buf.tell() > 0:
            self.upload()
        if self.parts:
            self.multipart_upload.complete(
                MultipartUpload={"Parts": self.parts})
        self.multipart_upload = None

    def terminate(self):
        self.multipart_upload.abort()
        self.multipart_upload = None

    def tell(self):
        return self.total_size

    def write(self, b):
        self.buf.write(b)
        self.total_size += len(b)
        if self.buf.tell() >= self.upload_part_size:
            self.upload()
        return len(b)

    def upload(self):
        part_number = len(self.parts) + 1
        part = self.multipart_upload.Part(part_number)
        self.buf.seek(0)
        logger.debug("uploading ... %d", part_number)
        upload = part.upload(Body=self.buf)
        logger.debug("uploading done: %s", upload["ETag"])
        self.parts.append({"ETag": upload["ETag"],
                           "PartNumber": part_number})
        self.buf = io.BytesIO()

    def __enter__(self):
         return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.terminate()
        else:
            self.close()
