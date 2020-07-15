# -*- coding: utf-8 -*-
import logging
import toml
import os
from munch import munchify

MIGRATE_TYPE_SECTION_NAME = "migrateType"
MIGRATE_TYPE = "type"

COMMON_SECTION_NAME = "common"
COMMON_SECRET_ID = "secretId"
COMMON_SECRET_KEY = "secretKey"
COMMON_REGION = "region"
COMMON_SUBAPPID = "subAppId"
COMMON_CONCURRENCY = "concurrency"
COMMON_SUPPORT_MEDIA_CLASSIFICATION = "supportMediaClassification"
COMMON_EXCLUDE_MEDIA_TYPE = "excludeMediaType"
COMMON_MIGRATE_DB_STORAGE_PATH = "migrateDbStoragePath"
COMMON_MIGRATE_RESULT_OUTPUT_PATH = "migrateResultOutputPath"

LOCAL_SECTION_NAME = "migrateLocal"
LOCAL_LOCAL_PATH = "localPath"
LOCAL_EXCLUDES = "excludes"

COS_SECTION_NAME = "migrateCos"
COS_REGION = "region"
COS_BUCKET = "bucket"
COS_SECRET_ID = "secretId"
COS_SECRET_KEY = "secretKey"
COS_PREFIX = "prefix"

URLLIST_SECTION_NAME = "migrateUrl"
URLLIST_PATH = "urllistPath"

ALI_SECTION_NAME = "migrateAli"
AWS_SECTION_NAME = "migrateAws"
QINIU_SECTION_NAME = "migrateQiniu"

OSS_REGION = "region"
OSS_BUCKET = "bucket"
OSS_AK = "accessKeyId"
OSS_SK = "accessKeySecret"
OSS_END_POINT = "endPoint"
OSS_PREFIX = "prefix"

MIGRATE_FROM_LOCAL = "migrateLocal"
MIGRATE_FROM_COS = "migrateCos"
MIGRATE_FROM_URLLIST = "migrateUrl"
MIGRATE_FROM_AWS = "migrateAws"
MIGRATE_FROM_ALI = "migrateAli"
MIGRATE_FROM_QINIU = "migrateQiniu"

MAX_CONCURRENCY = 50

logger = logging.getLogger("cmd")

required_items = {
    MIGRATE_TYPE_SECTION_NAME: [
        MIGRATE_TYPE
    ],
    COMMON_SECTION_NAME: [
        COMMON_SECRET_ID, COMMON_SECRET_KEY, COMMON_REGION, 
        COMMON_CONCURRENCY, COMMON_SUPPORT_MEDIA_CLASSIFICATION,
        COMMON_EXCLUDE_MEDIA_TYPE, COMMON_MIGRATE_DB_STORAGE_PATH,
        COMMON_MIGRATE_RESULT_OUTPUT_PATH
    ],
    LOCAL_SECTION_NAME: [
        LOCAL_LOCAL_PATH, LOCAL_EXCLUDES
    ],
    COS_SECTION_NAME: [
        COS_REGION, COS_BUCKET, COS_SECRET_ID, COS_SECRET_KEY, COS_PREFIX
    ],
    URLLIST_SECTION_NAME: [
        URLLIST_PATH
    ],
    AWS_SECTION_NAME: [
        OSS_REGION, OSS_BUCKET, OSS_AK, OSS_SK, OSS_PREFIX
    ],
    ALI_SECTION_NAME: [
        OSS_BUCKET, OSS_AK, OSS_SK, OSS_END_POINT, OSS_PREFIX
    ],
    QINIU_SECTION_NAME: [
        OSS_BUCKET, OSS_AK, OSS_SK, OSS_END_POINT, OSS_PREFIX
    ]
}


class ConfigParser(object):
    '''配置解析类, 解析完成后获得迁移配置对象'''

    @staticmethod
    def parse(conf_path):
        if not os.path.exists(conf_path):
            raise Exception(
                'config file not exists: {conf}'.format(conf=conf_path))
        try:
            dict_config = toml.load(conf_path)

            if not ConfigParser.check_migrate_type_config(dict_config):
                raise Exception('Invalid config: migrateType')

            migrate_type = dict_config[MIGRATE_TYPE_SECTION_NAME][MIGRATE_TYPE]

            if not ConfigParser.check_common_config(dict_config):
                raise Exception('Invalid config: commonConfig')

            if migrate_type == MIGRATE_FROM_LOCAL:
                if not ConfigParser.check_migrate_local_config(dict_config):
                    raise Exception('Invalid config: migrateLocal')
            elif migrate_type == MIGRATE_FROM_COS:
                if not ConfigParser.check_migrate_cos_config(dict_config):
                    raise Exception('Invalid config: migrateCos')
            elif migrate_type == MIGRATE_FROM_URLLIST:
                if not ConfigParser.check_migrate_url_config(dict_config):
                    raise Exception('Invalid config: migrateUrl')
            elif migrate_type == MIGRATE_FROM_AWS:
                if not ConfigParser.check_migrate_competitor_config(
                        migrate_type, dict_config):
                    raise Exception('Invalid config: migrateAws')
            elif migrate_type == MIGRATE_FROM_ALI:
                if not ConfigParser.check_migrate_competitor_config(
                        migrate_type, dict_config):
                    raise Exception('Invalid config: migrateAli')
            elif migrate_type == MIGRATE_FROM_QINIU:
                if not ConfigParser.check_migrate_competitor_config(
                        migrate_type, dict_config):
                    raise Exception('Invalid config: migrateQiniu')
            else:
                raise Exception(
                    'Unsupported migrateType: {migrate_type}'.format(
                        migrate_type=migrate_type))

            return munchify(dict_config)
        except Exception as e:
            raise e

    @staticmethod
    def check_migrate_type_config(dict_config):
        if MIGRATE_TYPE_SECTION_NAME in dict_config:
            if MIGRATE_TYPE in dict_config[MIGRATE_TYPE_SECTION_NAME]:
                return True

        return False

    @staticmethod
    def check_common_config(dict_config):
        if COMMON_SECTION_NAME not in dict_config:
            return False

        common_config = dict_config[COMMON_SECTION_NAME]

        if not ConfigParser.check_items_exist(COMMON_SECTION_NAME, common_config):
            return False

        if COMMON_SUBAPPID not in dict_config[COMMON_SECTION_NAME]:
            dict_config[COMMON_SECTION_NAME][COMMON_SUBAPPID] = 0

        concurrency = int(common_config[COMMON_CONCURRENCY])
        if concurrency <= 0 or concurrency >= MAX_CONCURRENCY:
            logger.error("legal concurrency is [1, 50]")
            return False

        migrate_db_storage_path = dict_config[COMMON_SECTION_NAME][
            COMMON_MIGRATE_DB_STORAGE_PATH]
        dict_config[COMMON_SECTION_NAME][
            COMMON_MIGRATE_DB_STORAGE_PATH] = os.path.abspath(
                migrate_db_storage_path)

        migrate_result_output_path = dict_config[COMMON_SECTION_NAME][
            COMMON_MIGRATE_RESULT_OUTPUT_PATH]
        dict_config[COMMON_SECTION_NAME][
            COMMON_MIGRATE_RESULT_OUTPUT_PATH] = os.path.abspath(
                migrate_result_output_path)

        return True

    @staticmethod
    def check_migrate_local_config(dict_config):
        if LOCAL_SECTION_NAME not in dict_config:
            return False

        local_config = dict_config[LOCAL_SECTION_NAME]

        if not ConfigParser.check_items_exist(LOCAL_SECTION_NAME, local_config):
            return False

        local_path = dict_config[LOCAL_SECTION_NAME][LOCAL_LOCAL_PATH]

        dict_config[LOCAL_SECTION_NAME][LOCAL_LOCAL_PATH] = os.path.abspath(
            local_path)

        excludes = local_config[LOCAL_EXCLUDES]

        for i in range(len(excludes)):
            dict_config[LOCAL_SECTION_NAME][LOCAL_EXCLUDES][
                i] = os.path.abspath(excludes[i])

        return True

    @staticmethod
    def check_migrate_cos_config(dict_config):
        if COS_SECTION_NAME not in dict_config:
            return False

        cos_config = dict_config[COS_SECTION_NAME]

        if not ConfigParser.check_items_exist(COS_SECTION_NAME, cos_config):
            return False

        return True

    @staticmethod
    def check_migrate_url_config(dict_config):
        if URLLIST_SECTION_NAME not in dict_config:
            return False

        urllist_config = dict_config[URLLIST_SECTION_NAME]

        if not ConfigParser.check_items_exist(URLLIST_SECTION_NAME, urllist_config):
            return False

        urllist_config[URLLIST_PATH] = os.path.abspath(
            urllist_config[URLLIST_PATH])

        return True

    @staticmethod
    def check_migrate_competitor_config(migrate_type, dict_config):
        sectionName = ""

        if migrate_type == MIGRATE_FROM_AWS:
            sectionName = AWS_SECTION_NAME
        elif migrate_type == MIGRATE_FROM_ALI:
            sectionName = ALI_SECTION_NAME
        elif migrate_type == MIGRATE_FROM_QINIU:
            sectionName = QINIU_SECTION_NAME
        else:
            raise Exception('Unsupported migrateType: {migrateType}'.format(
                migrateType=migrate_type))

        if sectionName not in dict_config:
            return False

        oss_config = dict_config[sectionName]

        return ConfigParser.check_items_exist(sectionName, oss_config)

    @staticmethod
    def check_items_exist(section, dict_config):
        if section not in required_items:
            logger.error('{section} not found in required_items'.format(section=section))
            return False

        for i in range(len(required_items[section])):
            item = required_items[section][i]
            if item not in dict_config:
                logger.error('item: {item}, not found in section: {section}'.format(
                    item=item, section=section
                ))
                return False

        return True
