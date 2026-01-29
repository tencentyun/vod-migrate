#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import traceback
sys.path.append("..")
import logging
from qcloud_vod_migrate.config import ConfigParser
from qcloud_vod_migrate.manager import MigrateManager, MIGRATE_INIT
from qcloud_vod_migrate.execute import TaskProducer, TaskConsumer
from qcloud_vod_migrate.util import fs_coding
from six import PY2

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s %(filename)s [line:%(lineno)d] %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    try:
        conf_path = './config_template.toml'
        config = ConfigParser.parse(conf_path)

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
        traceback.print_exc()
        raise e
