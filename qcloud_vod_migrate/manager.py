# -*- coding: utf-8 -*-
import logging
import os
import json
import time
import datetime
import sys
import threading
from qcloud_vod_migrate.util import get_file_md5
from qcloud_vod_migrate.util import fs_coding
from sqlalchemy import create_engine, Column, Integer, String, text, TIMESTAMP, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
from six import text_type, PY2, PY3

if sys.version >= '3.2.':
    localtimezone = datetime.timezone(
        datetime.timedelta(seconds=-time.timezone), time.tzname[0])
    utctimezone = datetime.timezone.utc
else:
    from dateutil import tz
    localtimezone = tz.tzlocal()
    utctimezone = tz.gettz('UTC')

MIGRATE_TASK_INIT = "init"
MIGRATE_TASK_SUCCESS = "success"
MIGRATE_TASK_FAIL = "fail"
MIGRATE_TASK_EXCLUDE = "exclude"

MIGRATE_INIT = "init"
MIGRATE_RUNNING = "running"
MIGRATE_FINISHED = "finished"

MIGRATE_RESULT_FILE = "vod_migrate_result.txt"
MIGRATE_DB = "migrate.db"
MIGRATE_RECORDS_TABLE = "records"
MIGRATE_SESSION_TABLE = "session"

MAX_FETCH_NUM = 1000

logger = logging.getLogger("cmd")

Base = declarative_base()
Session = sessionmaker()


class MigrateStatus(Base):
    '''迁移状态，标识迁移阶段'''

    __tablename__ = MIGRATE_SESSION_TABLE

    id = Column(Integer, primary_key=True, autoincrement=True)
    status = Column(String(16))
    config_md5 = Column(String(64))
    create_time = Column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP'))
    update_time = Column(
        TIMESTAMP,
        server_default=text('CURRENT_TIMESTAMP'),
        onupdate=func.now())


class MigrateRecord(Base):
    '''迁移记录，与文件关联'''

    __tablename__ = MIGRATE_RECORDS_TABLE

    id = Column(Integer, primary_key=True, autoincrement=True)
    migrate_type = Column(String(32))
    filename = Column(Text, server_default='')
    mtime = Column(Integer)
    filesize = Column(Integer)
    etag = Column(String(128), server_default='')
    file_id = Column(String(128), server_default='')
    vod_url = Column(String(256), server_default='')
    status = Column(String(16))
    err_msg = Column(String(256), nullable=False, server_default='')
    create_time = Column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP'))
    update_time = Column(
        TIMESTAMP,
        server_default=text('CURRENT_TIMESTAMP'),
        onupdate=func.now())


class MigrateManager(object):

    def __init__(self, conf):
        self.conf = conf
        self.execute_begin_time = None
        self.lock = threading.Lock()
        self.engine = create_engine(
            r'sqlite:///{db_path}?check_same_thread=False'.format(
                db_path=os.path.join(conf.common.migrateDbStoragePath,
                                     MIGRATE_DB)))
        self.total_num = 0
        self.success_num = 0
        self.fail_num = 0
        Base.metadata.create_all(self.engine, checkfirst=True)
        Session.configure(bind=self.engine)

    def check_config_file(self, config_path):
        md5_str = get_file_md5(config_path)
        session = Session()
        count = session.query(MigrateStatus).filter(
            MigrateStatus.config_md5 == md5_str).count()
        if count == 1:
            return True

        return False

    def update_execute_begin_time(self):
        self.execute_begin_time = datetime.datetime.utcnow().replace(
            microsecond=0)

    def get_migrate_record(self, migrate_type, filename):
        session = Session()
        record = session.query(MigrateRecord).filter(
            MigrateRecord.migrate_type == migrate_type).filter(
                MigrateRecord.filename == filename).first()

        return record

    def get_unfinished_migrate_records(self, migrate_type):
        session = Session()
        try:
            records = session.query(MigrateRecord).filter(
                MigrateRecord.migrate_type == migrate_type).filter(
                    MigrateRecord.status != MIGRATE_TASK_SUCCESS).filter(
                        MigrateRecord.update_time < self.execute_begin_time).limit(
                            MAX_FETCH_NUM).all()
            return records
        except Exception as e:
            logger.error(e)
            raise e

    def get_migrate_results(self, page_index, page_size):
        try:
            session = Session()
            records = session.query(MigrateRecord).filter(
                MigrateRecord.migrate_type == self.conf.migrateType.type).limit(
                    page_size).offset(page_index * page_size)

            return records
        except Exception as e:
            logger.error(e)
            raise e

    def save_migrate_record(self, record):
        session = Session()
        r = session.query(MigrateRecord).filter(
            MigrateRecord.migrate_type == record.migrate_type).filter(
                MigrateRecord.filename == record.filename).first()
        if r is not None:
            r.mtime = record.mtime
            r.filesize = record.filesize
            r.etag = record.etag
            r.file_id = record.file_id
            r.vod_url = record.vod_url
            r.status = record.status
            r.err_msg = record.err_msg
        else:
            session.add(record)

        session.commit()

    def init_migrate_status(self, config_path):
        session = Session()
        md5_str = get_file_md5(config_path)
        migrate_session = MigrateStatus(status=MIGRATE_INIT, config_md5=md5_str)
        session.add(migrate_session)
        session.commit()

    def get_migrate_status(self):
        session = Session()
        s = session.query(MigrateStatus).order_by(
            MigrateStatus.id.desc()).first()
        if s is None:
            return MIGRATE_INIT
        else:
            return s.status

    def update_migrate_status(self, status):
        try:
            session = Session()
            s = session.query(MigrateStatus).order_by(
                MigrateStatus.id.desc()).first()
            if s is not None:
                s.status = status
            else:
                migrate_session = MigrateStatus(status=status)
                session.add(migrate_session)

            session.commit()
        except Exception as e:
            logger.error(e)
            raise e

    def init_migrate_db(self):
        '''迁移启动时，检测为MIGRATE_INIT状态，则清空整个db：用于清理历史执行扫描时，命令被异常中断，而残留在db中的迁移记录'''

        Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine, checkfirst=True)

    def output_migrate_results(self):
        '''输出迁移结果'''

        if self.total_num == 0:
            return

        page_count = int(self.total_num / MAX_FETCH_NUM)
        page_index = 0

        result_file = os.path.join(self.conf.common.migrateResultOutputPath,
                                   MIGRATE_RESULT_FILE)

        try:
            if PY3:
                f = open(file=result_file, mode='w', encoding='utf-8')
            else:
                f = open(name=result_file, mode='w')

            while page_index <= page_count:
                results = self.get_migrate_results(page_index, MAX_FETCH_NUM)
                page_index += 1

                lines = []
                for record in results:
                    result = {}
                    result['id'] = record.id
                    result['migrateType'] = record.migrate_type
                    result['filename'] = record.filename
                    result['lastModified'] = record.mtime
                    result['filesize'] = record.filesize
                    result['etag'] = record.etag
                    result['fileId'] = record.file_id
                    result['mediaUrl'] = record.vod_url
                    result['status'] = record.status
                    result['errMsg'] = record.err_msg
                    result['updateTime'] = record.update_time.replace(
                        tzinfo=utctimezone).astimezone(localtimezone).strftime(
                            "%Y-%m-%d %H:%M:%S")
                    line = json.dumps(result, ensure_ascii=False) + '\n'
                    if PY2:
                        if not isinstance(line, text_type):
                            line = line.decode(fs_coding)
                        line = line.encode('utf-8')
                    lines.append(line)

                f.writelines(lines)
                f.flush()

            f.close()
            logger.info("migrate result: {file}".format(file=result_file))
        except Exception as e:
            logger.error(e)
            raise e

    def init_counter(self):
        session = Session()
        self.total_num = session.query(MigrateRecord).filter(
            MigrateRecord.migrate_type == self.conf.migrateType.type).count()
        self.success_num = session.query(MigrateRecord).filter(
            MigrateRecord.migrate_type == self.conf.migrateType.type).filter(
                MigrateRecord.status == MIGRATE_TASK_SUCCESS).count()

    def increse_counter(self, isSuccess):
        self.lock.acquire()
        if isSuccess:
            self.success_num += 1
        else:
            self.fail_num += 1
        self.lock.release()

    def output_migrate_progress(self):
        if self.total_num > 0:
            percent = '{:.2%}'.format(
                (self.success_num + self.fail_num) / float(self.total_num))
            logger.info(
                u"当前迁移进度：{per}, 总量：{total_num}, 成功数：{success_num}, 失败数：{fail_num}"
                .format(
                    per=percent,
                    total_num=self.total_num,
                    success_num=self.success_num,
                    fail_num=self.fail_num))
