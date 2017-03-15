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

master = 0

# Start the master and block until it's started
if rank == master:
  p = Popen(["./task-roles.sh", "master"], stderr=PIPE)
  logging.info("{0} master".format(rank)) 
  master_started = True
else:
  master_started = None
master_started = comm.bcast(master_started, root=master)

# Start workers in each process
logging.info("{0} worker".format(rank)) 
p = Popen(["./task-roles.sh", "worker"], stdin=PIPE, stdout=PIPE, stderr=PIPE)
time.sleep(10)
if p.poll() is not None:
  logging.warn("Worker process ({0}) has died".format(rank))

if rank == master:
  
  # Wait until the other worker processes have responded
  v = 0
  logging.info("Waiting for workers to respond")
  for i in range(2, size):
    v += comm.recv(source=i)
    logging.info(v)
  logging.info("Workers have responded")

  # Execute the client role
  subprocess.call(["./task-roles.sh", "client"])
  client_finished = True
else:
  # Tell the master that you've started
  comm.send(1, dest=master)
  client_finished = None
client_finished = comm.bcast(client_finished, root=master)

if rank != master:
  subprocess.call(["./task-roles.sh", "worker-cleanup"])

