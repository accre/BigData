#!/usr/bin/env python
from __future__ import print_function
import logging
import os
import sys
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

keys = [
    "JOB_HOME",
    "APP",
    "SPARK_DAEMON_MEMORY",
    "SPARK_WORKER_CORES",
    "SPARK_WORKER_MEMORY",
    "SPARK_EXECUTOR_CORES",
    "SPARK_EXECUTOR_MEMORY",
    "MASTER_URL",
    "SPARK_MASTER_HOST",
    "SPARK_MASTER_PORT",
    "SPARK_MASTER_WEBUI_PORT",
    ]

data = {k: os.getenv(k, "") for k in keys}

for kv in data.items():
  logging.info("{0}={1}".format(*kv))

#logging.info("To tunnel to MasterUI and JobUI -> ssh \
#    -L {SPARK_MASTER_WEBUI_PORT}:{MASTER_HOST}:{SPARK_MASTER_WEBUI_PORT} \
#    -L 4040:{MASTER_HOST}:4040 \
#    $USER@login.accre.vanderbilt.edu"
#    )


