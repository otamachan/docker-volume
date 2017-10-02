#!/usr/bin/env python
# -*- coding: utf-8 -*-

import SimpleHTTPServer
import SocketServer
import argparse
import datetime
import glob
import logging
import os
import re
import signal
import subprocess
import sys
import tarfile
import urlparse

import boto3
import s3

UPLOAD_PART_SIZE = 100 * 1024**2
DOWNLOAD_PART_SIZE = 100 * 1024**2


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def should_exclude(filename, exclude_list):
    for exclude in exclude_list:
        if re.search(exclude, filename):
            return True
    return False


class Config(object):
    def __init__(self, argv):
        parser = argparse.ArgumentParser()
        parser.add_argument('--port', default=8000, type=int)
        parser.add_argument('--path', required=True, type=str)
        parser.add_argument('--dest', required=True, type=str)
        parser.add_argument('--mode', default=None, type=str)
        parser.add_argument('--owner', default=None, type=str)
        parser.add_argument('--compresslevel', default=9, type=int)
        parser.add_argument('--exclude', default=[], nargs='*')
        parser.add_argument('--no-restore', action='store_true')
        parser.add_argument('--no-backup', action='store_true')
        args = parser.parse_args(argv[:])
        self.port = args.port
        self.path = args.path
        self.dest = args.dest
        self.mode = args.mode
        self.owner = args.owner
        self.compresslevel = args.compresslevel
        self.exclude = args.exclude
        self.no_restore = args.no_restore
        self.no_backup = args.no_backup


class Volume(object):
    def __init__(self, config):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.config = config
        signal.signal(signal.SIGINT, self.signal)
        signal.signal(signal.SIGTERM, self.signal)

    def backup(self, callback=None):
        suffix = datetime.datetime.now().strftime("-%Y%m%d-%H%M%S") + '.tar.gz'
        path = self.config.path
        exclude_list = self.config.exclude
        dest = self.config.dest
        parts = urlparse.urlparse(dest)
        if parts.scheme not in ('s3', 'file'):
            raise RuntimeError("Not supported scheme: {0}".format(dest))
        backup_file = parts.path + suffix
        self.logger.info("Start backup: %s to %s", path, backup_file)
        if parts.scheme == 's3':
            def open_func():
                return s3.open(parts.netloc,
                               backup_file[1:],
                               "wb",
                               upload_part_size=UPLOAD_PART_SIZE)
        elif parts.scheme == 'file':
            dest_path = os.path.join(parts.netloc, backup_file)
            dirname = os.path.dirname(dest_path)
            if not os.path.exists(dirname):
                os.makedirs(dirname)

            def open_func():
                return open(dest_path, "wb")
        else:
            raise RuntimeError("Not supported scheme: {0}".
                               format(dest))
        with open_func() as fileobj:
            tar = tarfile.open(fileobj=fileobj,
                               mode="w:gz",
                               compresslevel=self.config.compresslevel)
            for root, dirs, files in os.walk(path):
                for f in files + dirs:
                    if root == path:
                        arcname = f
                    else:
                        arcname = os.path.join(root[len(path)+1:], f)
                    if not should_exclude(arcname,
                                          exclude_list):
                        try:
                            filename = os.path.join(root, f)
                            if callback is not None:
                                callback(filename)
                            tar.add(filename,
                                    arcname=arcname,
                                    recursive=False)
                        except IOError:
                            pass
            tar.close()
        self.logger.info("Done backup")

    def restore(self):
        path = self.config.path
        self.logger.info("Restoring to {0}".format(path))
        if not os.path.exists(path):
            os.makedirs(path)
        if self.config.mode is not None:
            self.logger.info("chmod {0}".format(self.config.mode))
            subprocess.call(['chmod', self.config.mode, path])
        if self.config.owner is not None:
            self.logger.info("chown {0}".format(self.config.owner))
            subprocess.call(['chown', self.config.owner, path])
        dest = self.config.dest
        parts = urlparse.urlparse(dest)
        open_func = None
        if parts.scheme == 's3':
            client = boto3.client('s3')
            objects = client.list_objects(Bucket=parts.netloc,
                                          Prefix=parts.path[1:])
            if 'Contents' in objects:
                keys = sorted([c['Key'] for c in objects['Contents']])
                if keys:
                    key = keys[-1]

                    def open_func():
                        return s3.open(parts.netloc,
                                       key,
                                       "rb",
                                       buffer_size=DOWNLOAD_PART_SIZE)
                    self.logger.info("Restoring from s3://{0}/{1}".
                                     format(parts.netloc, key))
        elif parts.scheme == 'file':
            src_file = os.path.join(parts.netloc, parts.path)
            files = sorted(glob.glob(src_file + '*'))
            if files:
                filename = files[-1]

                def open_func():
                    return open(filename, "rb")
                self.logger.info("Restoring from file://{0}".format(filename))
        else:
            raise RuntimeError("Not supported scheme: {0}".
                               format(dest))
        if open_func is not None:
            with open_func() as fileobj:
                tar = tarfile.open(fileobj=fileobj,
                                   mode='r:gz')
                tar.extractall(path)
                tar.close()

    def signal(self, sig, stack):
        self.logger.info("Recieved signal: %d", sig)
        raise SystemExit('Exiting')


class ServerHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
    def do_GET(self):
        self.wfile.write("GET\n")
        self.send_response(200)

    def do_POST(self):
        self.log_message('POST recieved')
        try:
            def callback(f):
                self.wfile.write(f + "\n")
            self.server.volume.backup(callback)
            self.wfile.write("BACKUP DONE\n")
            self.send_response(200)
        except Exception as err:
            import traceback
            self.wfile.write(traceback.format_exc())
            self.send_response(500)
            raise


class Server(SocketServer.TCPServer):
    allow_reuse_address = True

config = Config(sys.argv[1:])
volume = Volume(config)
if not config.no_restore:
    volume.restore()

Handler = ServerHandler
httpd = Server(("", config.port), Handler)
httpd.volume = volume

logger.info("Server started port:%d", config.port)
try:
    httpd.serve_forever()
finally:
    if not config.no_backup:
        volume.backup()
    logger.info("Finished")
