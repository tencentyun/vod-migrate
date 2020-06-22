# -*- coding:utf-8 -*-

import logging
import os
import sys
import time
import datetime
if sys.version_info[0] == 3:
    from urllib.request import urlopen
    from urllib.parse import quote
else:
    from urllib2 import urlopen
    from urllib import quote

from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
from qcloud_vod.common import FileUtil
from qcloud_vod_migrate.manager import MIGRATE_INIT, MIGRATE_RUNNING, MIGRATE_FINISHED, MIGRATE_TASK_SUCCESS, MIGRATE_TASK_FAIL, MigrateRecord
from qcloud_vod_migrate.upload import VodUploader
from qcloud_vod_migrate.config import MIGRATE_FROM_LOCAL, MIGRATE_FROM_URLLIST, MIGRATE_FROM_COS, MIGRATE_FROM_AWS, MIGRATE_FROM_ALI, MIGRATE_FROM_QINIU
from qcloud_vod_migrate.util import to_printable_str
from qcloud_vod_migrate.util import fs_coding
from qcloud_vod.vod_upload_client import VodUploadClient
from qcloud_vod.model import VodUploadRequest
from qcloud_cos import CosConfig, CosS3Client
from six import text_type
import boto3.session
import oss2
import qiniu

logger = logging.getLogger("cmd")

# 媒体格式所属分类配置
media_classification_config = [{
    "class":
        "video",
    "mediaTypeList": [
        "mp4", "flv", "wmv", "asf", "rm", "rmvb", "mpg", "mpeg", "3gp", "mov",
        "webm", "mkv", "avi"
    ],
}, {
    "class": "audio",
    "mediaTypeList": ["mp3", "m4a", "flac", "ogg", "wav"],
}, {
    "class":
        "image",
    "mediaTypeList": [
        "jpg", "jpeg", "png", "gif", "bmp", "tiff", "ai", "cdr", "eps"
    ],
}]

max_retry_times = 3


class TaskProducer(object):
    '''任务生产者， 通过扫描存储源，取得需要迁移文件列表，生成相应的迁移任务'''

    def __init__(self, migrate_manager):
        self.conf = migrate_manager.conf
        self.migrate_type = self.conf.migrateType.type
        self.task_list = []
        self.executor = ThreadPoolExecutor(max_workers=self.conf.common.concurrency)
        self.start_time = int(time.time())
        self.migrate_manager = migrate_manager

    def get_media_classification(self, filename):
        file_type = FileUtil.get_file_type(filename)
        for classification in iter(media_classification_config):
            for media_type in iter(classification["mediaTypeList"]):
                if file_type == media_type:
                    return classification["class"]
        return ""

    def is_support_media_classification(self, filename):
        classification = self.get_media_classification(filename)
        for c in self.conf.common.supportMediaClassification:
            if c == classification:
                return True

        return False

    def is_exclude_media_type(self, filename):
        file_type = FileUtil.get_file_type(filename)
        for media_type in iter(self.conf.common.excludeMediaType):
            if file_type == media_type:
                return True
        return False

    def is_excludes(self, filename):
        '''检查文件的目录是否被排除'''

        for exclude_path in iter(self.conf.migrateLocal.excludes):
            filepath = os.path.dirname(filename)
            if filepath.startswith(exclude_path):
                return True
        return False

    def save_record(self, record):
        '''保存迁移结果'''

        try:
            self.migrate_manager.save_migrate_record(record)
            return
        except Exception as e:
            logger.error(e)
            raise e

    def need_to_migrate(self, filename):
        '''检查该文件的类别（视频、音频、图片）是否需要迁移；检查该文件的类型（mp4、flv、mp3等）是否被排除'''

        if self.migrate_type == MIGRATE_FROM_LOCAL:
            if self.is_excludes(filename):
                return False

        if not self.is_support_media_classification(filename):
            return False

        if self.is_exclude_media_type(filename):
            return False

        return True

    def need_to_build(self):
        '''是否需要执行扫描，构建迁移任务'''

        status = self.migrate_manager.get_migrate_status()
        if status != MIGRATE_INIT:
            logger.info("tasks have already built, skip")
            return False

        return True

    def run(self):
        '''扫描存储源， 生成迁移任务'''

        if not self.need_to_build():
            return

        logger.info("build tasks")

        if self.migrate_type == MIGRATE_FROM_LOCAL:
            try:
                for root, _, files in os.walk(
                        self.conf.migrateLocal.localPath, topdown=True):
                    for name in files:
                        local_file = os.path.join(root, name)
                        if not isinstance(local_file, text_type):
                            local_file = local_file.decode(fs_coding)
                        if self.need_to_migrate(local_file):
                            file_info = os.stat(local_file)
                            record = MigrateRecord(
                                migrate_type=self.migrate_type,
                                filename=local_file,
                                mtime=int(file_info.st_mtime),
                                filesize=file_info.st_size,
                                etag="",
                                status=MIGRATE_INIT)
                            self.save_record(record)
            except Exception as e:
                logger.error(e)
                raise e
        elif self.migrate_type == MIGRATE_FROM_URLLIST:
            try:
                for url in open(self.conf.migrateUrl.urllistPath):
                    if not isinstance(url, text_type):
                        url = url.decode(fs_coding)
                    if self.need_to_migrate(url):
                        record = MigrateRecord(
                            migrate_type=self.migrate_type,
                            filename=url,
                            etag="",
                            status=MIGRATE_INIT)
                        self.save_record(record)
            except Exception as e:
                logger.error(e)
                raise e
        elif self.migrate_type == MIGRATE_FROM_COS:
            cos_config = CosConfig(
                Region=self.conf.migrateCos.region,
                SecretId=self.conf.migrateCos.secretId,
                SecretKey=self.conf.migrateCos.secretKey)
            cos_client = CosS3Client(cos_config)
            marker = ""
            is_truncated = 'true'
            while is_truncated == 'true':
                for i in range(max_retry_times):
                    try:
                        res = cos_client.list_objects(
                            Bucket=self.conf.migrateCos.bucket,
                            Prefix=self.conf.migrateCos.prefix,
                            Delimiter='',
                            MaxKeys=1000,
                            Marker=marker)

                        if 'Contents' in res:
                            for file in res['Contents']:
                                if not isinstance(file['Key'], text_type):
                                    file['Key'] = file['Key'].decode('utf-8')
                                if self.need_to_migrate(file['Key']):
                                    record = MigrateRecord(
                                        migrate_type=self.migrate_type,
                                        filename=file['Key'],
                                        etag=file['ETag'],
                                        filesize=int(file['Size']),
                                        status=MIGRATE_INIT)
                                    self.save_record(record)
                        if 'NextMarker' in res:
                            marker = res['NextMarker']
                        if 'IsTruncated' in res:
                            is_truncated = res['IsTruncated']
                        break
                    except Exception as e:
                        time.sleep(1 << i)
                        logger.error(e)
                        if i + 1 == max_retry_times:
                            raise e
        elif self.migrate_type == MIGRATE_FROM_AWS:
            try:
                session = boto3.session.Session(
                    region_name=self.conf.migrateAws.region,
                    aws_access_key_id=self.conf.migrateAws.accessKeyId,
                    aws_secret_access_key=self.conf.migrateAws.accessKeySecret)
                s3 = session.resource('s3')
                bucket = s3.Bucket(self.conf.migrateAws.bucket)
                prefix = self.conf.migrateAws.prefix

                for obj in bucket.objects.all():
                    if not isinstance(obj.key, text_type):
                        obj.key = obj.key.decode('utf-8')
                    if prefix != '':
                        if not obj.key.startswith(prefix):
                            continue
                    if self.need_to_migrate(obj.key):
                        record = MigrateRecord(
                            migrate_type=self.migrate_type,
                            filename=obj.key,
                            etag=obj.e_tag,
                            filesize=obj.size,
                            status=MIGRATE_INIT)
                        self.save_record(record)
            except Exception as e:
                logger.error(e)
                raise e
        elif self.migrate_type == MIGRATE_FROM_ALI:
            try:
                auth = oss2.Auth(
                    self.conf.migrateAli.accessKeyId,
                    self.conf.migrateAli.accessKeySecret)

                bucket = oss2.Bucket(
                    auth,
                    self.conf.migrateAli.endPoint,
                    self.conf.migrateAli.bucket)

                for obj in oss2.ObjectIterator(
                        bucket=bucket, prefix=self.conf.migrateAli.prefix):
                    if not isinstance(obj.key, text_type):
                        obj.key = obj.key.decode('utf-8')
                    if self.need_to_migrate(obj.key):
                        record = MigrateRecord(
                            migrate_type=self.migrate_type,
                            filename=obj.key,
                            etag=obj.etag,
                            filesize=obj.size,
                            status=MIGRATE_INIT)
                        self.save_record(record)
            except Exception as e:
                logger.error(e)
                raise e
        elif self.migrate_type == MIGRATE_FROM_QINIU:
            auth = qiniu.Auth(
                self.conf.migrateQiniu.accessKeyId,
                self.conf.migrateQiniu.accessKeySecret)
            bucket = qiniu.BucketManager(auth)

            marker = None
            prefix = self.conf.migrateQiniu.prefix
            if prefix == '':
                prefix = None
            eof = False
            while not eof:
                for i in range(max_retry_times):
                    try:
                        res, eof, _ = bucket.list(
                            bucket=self.conf.migrateQiniu.bucket,
                            prefix=prefix,
                            limit=1000,
                            marker=marker)

                        if 'items' in res:
                            for file in res['items']:
                                if not isinstance(file['key'], text_type):
                                    file['key'] = file['key'].decode('utf-8')
                                if self.need_to_migrate(file['key']):
                                    record = MigrateRecord(
                                        migrate_type=self.migrate_type,
                                        filename=file['key'],
                                        etag=file['md5'],
                                        filesize=int(file['fsize']),
                                        status=MIGRATE_INIT)
                                    self.save_record(record)
                        if 'marker' in res:
                            marker = res['marker']
                        break
                    except Exception as e:
                        time.sleep(1 << i)
                        logger.error(e)
                        if i + 1 == max_retry_times:
                            raise e

        self.migrate_manager.update_migrate_status(MIGRATE_RUNNING)
        return


class TaskConsumer(object):
    '''任务消费类，负责拉取未完成的任务提交到线程池'''

    def __init__(self, migrate_manager):
        self.conf = migrate_manager.conf
        self.migrate_type = self.conf.migrateType.type
        self.task_list = []
        self.executor = ThreadPoolExecutor(max_workers=self.conf.common.concurrency)
        self.start_time = int(time.time())
        self.migrate_manager = migrate_manager

    def add_task(self, task):
        '''将迁移任务提交到线程池中'''

        try:
            running_task = self.executor.submit(task.run)
            self.task_list.append(running_task)
        except Exception as e:
            logger.error(e)
            raise e

    def run(self):
        '''拉取迁移任务并提交到线程池'''

        self.migrate_manager.update_execute_begin_time()
        self.migrate_manager.init_counter()

        while True:
            time.sleep(1)
            records = self.migrate_manager.get_unfinished_migrate_records(
                self.migrate_type)
            if len(records) == 0:
                break
            for record in records:
                task = Task(
                    conf=self.conf,
                    migrate_manager=self.migrate_manager,
                    record=record)
                self.add_task(task)

                logger.info("add migrate task: {filename}".format(
                    filename=to_printable_str(record.filename)))

            self.wait_tasks_over()

        logger.info("tasks finished")
        self.migrate_manager.update_migrate_status(MIGRATE_FINISHED)
        self.migrate_manager.output_migrate_results()

        return

    def wait_tasks_over(self):
        '''阻塞等待，直到迁移任务完成'''

        wait(self.task_list, return_when=ALL_COMPLETED)
        self.task_list[:] = []
        return


class Task(object):
    '''迁移任务类，真正执行迁移操作'''

    def __init__(self, conf, migrate_manager, record):
        self.conf = conf
        self.migrate_type = conf.migrateType.type
        self.migrate_manager = migrate_manager
        self.record = record
        self.vod_client = VodUploadClient(conf.common.secretId,
                                          conf.common.secretKey)
        self.vod_uploader = VodUploader(conf.common.secretId,
                                        conf.common.secretKey)

    def save_record(self):
        '''保存迁移结果'''

        try:
            self.migrate_manager.save_migrate_record(self.record)
            return
        except Exception as e:
            logger.error(e)

    def upload_file(self, filename):
        '''上传文件到vod'''

        if self.migrate_type == MIGRATE_FROM_LOCAL:
            try:
                request = VodUploadRequest()
                request.MediaFilePath = filename
                response = self.vod_client.upload(self.conf.common.region,
                                                  request)

                return response
            except Exception as e:
                logger.error("{file} upload failed: {error}".format(
                    file=to_printable_str(filename), error=e))
                raise e
        elif self.migrate_type == MIGRATE_FROM_URLLIST:
            try:
                r = urlopen(filename)
                if r.getcode() != 200:
                    logger.error("download failed: %d" % r.getcode())
                    return
                request = VodUploadRequest()
                request.MediaFilePath = filename
                response = self.vod_uploader.upload_from_buffer(
                    self.conf.common.region, request, r)

                return response
            except Exception as e:
                logger.error("{file} upload failed: {error}".format(
                    file=to_printable_str(filename), error=e))
                raise e
        elif self.migrate_type == MIGRATE_FROM_COS:
            try:
                cos_config = CosConfig(
                    Region=self.conf.migrateCos.region,
                    SecretId=self.conf.migrateCos.secretId,
                    SecretKey=self.conf.migrateCos.secretKey)
                cos_client = CosS3Client(cos_config)

                r = cos_client.get_object(self.conf.migrateCos.bucket, filename)
                request = VodUploadRequest()
                request.MediaFilePath = filename
                response = self.vod_uploader.upload_from_buffer(
                    self.conf.common.region, request,
                    r['Body'].get_raw_stream())

                return response
            except Exception as e:
                logger.error("{file} upload failed: {error}".format(
                    file=to_printable_str(filename), error=e))
                raise e
        elif self.migrate_type == MIGRATE_FROM_AWS:
            try:
                session = boto3.session.Session(
                    region_name=self.conf.migrateAws.region,
                    aws_access_key_id=self.conf.migrateAws.accessKeyId,
                    aws_secret_access_key=self.conf.migrateAws.accessKeySecret)
                s3 = session.resource('s3')
                bucket = s3.Bucket(self.conf.migrateAws.bucket)

                r = bucket.Object(filename).get()
                request = VodUploadRequest()
                request.MediaFilePath = filename
                response = self.vod_uploader.upload_from_buffer(
                    self.conf.common.region, request, r['Body'])

                return response
            except Exception as e:
                logger.error("{file} upload failed: {error}".format(
                    file=to_printable_str(filename), error=e))
                raise e
        elif self.migrate_type == MIGRATE_FROM_ALI:
            try:
                auth = oss2.Auth(
                    self.conf.migrateAli.accessKeyId,
                    self.conf.migrateAli.accessKeySecret)

                bucket = oss2.Bucket(
                    auth,
                    self.conf.migrateAli.endPoint,
                    self.conf.migrateAli.bucket)

                r = bucket.get_object(filename)
                request = VodUploadRequest()
                request.MediaFilePath = filename
                response = self.vod_uploader.upload_from_buffer(
                    self.conf.common.region, request, r)

                return response
            except Exception as e:
                logger.error("{file} upload failed: {error}".format(
                    file=to_printable_str(filename), error=e))
                raise e
        elif self.migrate_type == MIGRATE_FROM_QINIU:
            try:
                auth = qiniu.Auth(
                    self.conf.migrateQiniu.accessKeyId,
                    self.conf.migrateQiniu.accessKeySecret)

                end_point = self.conf.migrateQiniu.endPoint
                base_url = 'http://{end_point}/{key}'.format(
                    end_point=end_point, key=quote(to_printable_str(filename)))
                private_url = auth.private_download_url(base_url)

                r = urlopen(private_url)
                if r.getcode() != 200:
                    raise Exception('download: {key} failed, httpCode: {code}'.format(
                        key=filename, code=r.getcode()))

                request = VodUploadRequest()
                request.MediaFilePath = filename
                response = self.vod_uploader.upload_from_buffer(
                    self.conf.common.region, request, r)

                return response
            except Exception as e:
                logger.error("{file} upload failed: {error}".format(
                    file=to_printable_str(filename), error=e))
                raise e

        return

    def do_task(self):
        try:
            upload_result = self.upload_file(self.record.filename)
            if upload_result is None or upload_result.FileId is None or upload_result.MediaUrl is None:
                raise Exception(
                    "{file} upload failed".format(file=to_printable_str(self.record.filename)))

            self.record.file_id = upload_result.FileId
            self.record.vod_url = upload_result.MediaUrl
            self.record.err_msg = ""
            self.record.status = MIGRATE_TASK_SUCCESS
            self.save_record()
            self.report_task_result(is_success=True)
        except Exception as e:
            logger.error(e)
            self.record.status = MIGRATE_TASK_FAIL
            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            self.record.err_msg = "error: {error}, time: {time}".format(
                error=e,
                time=now
            )
            if not isinstance(self.record.err_msg, text_type):
                self.record.err_msg = self.record.err_msg.decode(fs_coding)
            self.save_record()
            self.report_task_result(is_success=False)

    def report_task_result(self, is_success):
        self.migrate_manager.increse_counter(is_success)

    def run(self):
        self.do_task()
        self.migrate_manager.output_migrate_progress()