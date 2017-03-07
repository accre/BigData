#!/usr/bin/env python
from __future__ import print_function

from mpi4py import MPI
import subprocess32 as subprocess
import os
import logging
import sys
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
root = 0
hostname = str(subprocess.check_output("hostname").strip())

# Get the master hostname 
if rank == 1:
  master = hostname
else:
  master = None

master = comm.bcast(master, root=1)
os.environ["SPARK_MASTER_HOST"] = hostname
os.environ["MASTER_URL"] = \
    "spark://{SPARK_MASTER_HOST}:{SPARK_MASTER_PORT}".format(**os.environ)
os.environ["MASTER_WEBUI_URL"] = \
    "spark://{SPARK_MASTER_HOST}:{SPARK_MASTER_WEBUI_PORT}".format(**os.environ)
logging.info("To tunnel to MasterUI and JobUI -> ssh \
    -L {SPARK_MASTER_WEBUI_PORT}:{MASTER_HOST}:{SPARK_MASTER_WEBUI_PORT} \
    -L 4040:{MASTER_HOST}:4040 \
    $USER@login.accre.vanderbilt.edu"
    )


if rank == 0:
  v = 0
  for i in range(2, size):
    v += comm.recv(source=i)
    logging.info(v)

  subprocess.call(["./task-roles.sh", "client"])
elif rank == 1:
  subprocess.call(["./task-roles.sh", "master"])
else:
  comm.send(1, dest=root)
  subprocess.call(["./task-roles.sh", "worker"])
