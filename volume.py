#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import boto3
import datetime
import logging
import os
import re
import signal
import SimpleHTTPServer
import SocketServer
import subprocess
import sys
import tarfile
import urlparse
import yaml
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def should_exclude(filename, exclude_list):
    for exclude in exclude_list:
        if re.search(exclude, filename):
            return True
    return False


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', default=8000, type=int)
    parser.add_argument('--config', required=True)
    return parser.parse_args()


class Volume(object):
    def __init__(self, config_path):
        self.logger = logging.getLogger(self.__class__.__name__)
        parts = urlparse.urlparse(config_path)
        if parts.scheme in ('http', 'https', 'ftp', 'file'):
            import urllib2
            response = urllib2.urlopen(config_path)
            config = response.read()
        elif parts.scheme == 's3':
            client = boto3.client('s3')
            response = client.get_object(Bucket=parts.netloc,
                                         Key=parts.path[1:])
            config = response['Body'].read()
        else:
            raise RuntimeError("Not supported scheme: {0}".format(config_path))
        self.config = yaml.load(config)
        self.tmp_dir = self.config.get('tmp', '/tmp')
        signal.signal(signal.SIGINT, self.signal)
        signal.signal(signal.SIGTERM, self.signal)

    def backup(self):
        suffix = datetime.datetime.now().strftime("-%Y%m%d-%H%M%S") + '.tar.gz'
        for backup in self.config['backups']:
            if 'path' not in backup:
                continue
            path = backup['path']
            exclude_list = backup.get('exclude', [])
            dest = backup['dest']
            parts = urlparse.urlparse(dest)
            if parts.scheme not in ('s3', ):
                raise RuntimeError("Not supported scheme: {0}".format(dest))
            backup_file = parts.path + suffix
            self.logger.info("Start backup: %s", path)
            tar_file = os.path.join(self.tmp_dir,
                                    os.path.basename(backup_file))
            tar = tarfile.open(tar_file, 'w:gz')
            try:
                for root, dirs, files in os.walk(path):
                    f_root = root[len(path):]
                    for f in files + dirs:
                        if not should_exclude(os.path.join(f_root, f),
                                              exclude_list):
                            try:
                                tar.add(os.path.join(root, f),
                                        arcname=os.path.join(f_root, f),
                                        recursive=False)
                            except IOError:
                                pass
                tar.close()
                if parts.scheme == 's3':
                    s3_params = backup.get('s3', {})
                    client = boto3.client('s3')
                    self.logger.info("Uploading %s to %s/%s",
                                     tar_file, parts.netloc, backup_file[1:])
                    client.upload_file(tar_file, parts.netloc, backup_file[1:],
                                       ExtraArgs=s3_params)
            finally:
                if os.path.exists(tar_file):
                    os.remove(tar_file)
            self.logger.info("Done backup: %s", path)

    def restore(self):
        for backup in self.config['backups']:
            if 'path' not in backup:
                continue
            path = backup['path']
            self.logger.info("Restoring to {0}".format(path))
            if not os.path.exists(path):
                os.makedirs(path)
            if 'chmod' in backup:
                self.logger.info("chmod {0}".format(backup['chmod']))
                subprocess.call(['chmod', backup['chmod'], path])
            if 'chown' in backup:
                self.logger.info("chown {0}".format(backup['chown']))
                subprocess.call(['chown', backup['chown'], path])
            dest = backup['dest']
            parts = urlparse.urlparse(dest)
            tar_file = None
            try:
                if parts.scheme == 's3':
                    client = boto3.client('s3')
                    objects = client.list_objects(Bucket=parts.netloc,
                                                  Prefix=parts.path[1:])
                    if 'Contents' in objects:
                        keys = sorted([c['Key'] for c in objects['Contents']])
                        if keys:
                            key = keys[-1]
                            tar_file = os.path.join(self.tmp_dir,
                                                    os.path.basename(key))
                            client.download_file(parts.netloc, key, tar_file)
                else:
                    raise RuntimeError("Not supported scheme: {0}".
                                       format(dest))
                if tar_file is not None:
                    self.logger.info("Restoring from {0}".format(key))
                    tar = tarfile.open(tar_file, 'r:gz')
                    tar.extractall(backup['path'])
                    tar.close()
            finally:
                if tar_file is not None:
                    if os.path.exists(tar_file):
                        os.remove(tar_file)

    def signal(self, sig, stack):
        self.logger.info("Recieved signal: %d", sig)
        self.backup()
        raise SystemExit('Exiting')


class ServerHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
    def do_GET(self):
        self.wfile.write("GET\n")
        self.send_response(200)

    def do_POST(self):
        self.log_message('POST recieved')
        try:
            self.server.volume.backup()
            self.wfile.write("BACKUP DONE\n")
            self.send_response(200)
        except Exception as err:
            import traceback
            self.wfile.write(traceback.format_exc())
            self.send_response(500)
            raise


class Server(SocketServer.TCPServer):
    allow_reuse_address = True

args = get_args()
volume = Volume(args.config)
volume.restore()

Handler = ServerHandler
httpd = Server(("", args.port), Handler)
httpd.volume = volume

logger.info("Server started port:%d", args.port)
try:
    httpd.serve_forever()
finally:
    logger.info("Finished")
