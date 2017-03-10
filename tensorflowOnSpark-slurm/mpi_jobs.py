#!/usr/bin/env python
from __future__ import print_function

from mpi4py import MPI
import subprocess
from subprocess import Popen, PIPE
import os
import time
import sys
import logging
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

client = 0
master = 1

# Start the master and block until its hostname is started
if rank == master:
  master_host = str(subprocess.check_output("hostname").strip())
else:
  master_host = None
master_host = comm.bcast(master_host, root=master)

# Set environment variables re spark master
os.environ["SPARK_MASTER_HOST"] = master_host 
os.environ["MASTER_URL"] = "spark://{0}:{1}".format(
    os.environ["SPARK_MASTER_HOST"],os.environ["SPARK_MASTER_PORT"]
    )
os.environ["MASTER_WEBUI_URL"] = "spark://{0}:{1}".format(
    os.environ["SPARK_MASTER_HOST"], os.environ["SPARK_MASTER_WEBUI_PORT"]
    )

# Start the master and block until it's started
if rank == master:
  p = Popen(["./task-roles.sh", "master"], stderr=PIPE)
  logging.info("{0} master".format(rank)) 
  master_started = True
else:
  master_started = None
master_started = comm.bcast(master_started, root=master)


if rank == client:
  # Wait until the master and each worker process have responded
  v = 0
  logging.info("Waiting for workers to respond")
  for i in range(2, size):
    v += comm.recv(source=i)
    logging.info(v)
  logging.info("Workers have responded")

  # Execute the client role
  subprocess.call(["./task-roles.sh", "client"])

elif rank != master:
  logging.info("{0} worker".format(rank)) 
  p = Popen(["./task-roles.sh", "worker"], stdin=PIPE, stdout=PIPE, stderr=PIPE)
  time.sleep(10)
  if p.poll() is not None:
    logging.warn("Process has died")
  comm.send(1, dest=client)
