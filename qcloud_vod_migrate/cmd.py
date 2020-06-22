# -*- coding:utf-8 -*-

import logging
import sys
from qcloud_vod_migrate.config import ConfigParser
from qcloud_vod_migrate.manager import MigrateManager, MIGRATE_INIT
from qcloud_vod_migrate.execute import TaskProducer, TaskConsumer
from qcloud_vod_migrate.util import fs_coding
from six import PY2

logger = logging.getLogger("cmd")
logger.setLevel(logging.INFO)

global res


def _main():
    global res
    res = 0
    try:
        if len(sys.argv) < 2:
            res = -1
            print('Usage: vodmigrate config.toml')
            return

        logger = logging.getLogger('cmd')
        console = logging.StreamHandler(sys.stdout)
        console.setFormatter(logging.Formatter('[%(asctime)s] %(message)s'))
        logger.setLevel(logging.INFO)
        logger.addHandler(console)

        conf_path = sys.argv[1]
        config = ConfigParser.parse(conf_path)

        # 兼容中文,避免str与unicode类型字符串拼接时报UnicodeDecodeError
        if PY2:
            config.common.region = config.common.region.encode(fs_coding)
            config.common.secretId = config.common.secretId.encode(fs_coding)
            config.common.secretKey = config.common.secretKey.encode(fs_coding)

        migrate_manager = MigrateManager(conf=config)
        if migrate_manager.get_migrate_status() == MIGRATE_INIT:
            migrate_manager.init_migrate_db()
            migrate_manager.init_migrate_status(conf_path)
        else:
            if not migrate_manager.check_config_file(conf_path):
                raise Exception("config has changed, exit!")

        task_producer = TaskProducer(
            migrate_manager=migrate_manager)
        task_producer.run()

        task_consumer = TaskConsumer(
            migrate_manager=migrate_manager)
        task_consumer.run()
    except Exception as e:
        logger.error(e)
        res = -2


if __name__ == '__main__':
    _main()
    global res
    sys.exit(res)