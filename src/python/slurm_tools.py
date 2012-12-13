import subprocess
import string
import time
import tempfile
import os
import re


class Config:
   
   def __init__(self):
      self.partitions = {}
      self.nodes = {}
      self.users = []

class Partition:
   
   def __init__(self):
      self.name = None
      self.nodes = {}

   def __repr__(self):
      return '<Partition: name=%s, nodes=%s>' % (self.name, ','.join(self.nodes))

class Node:
   
   def __init__(self):
      self.name = None
      self.state = None
      self.cpus = None
      self.memory = None

   def __repr__(self):
      return '<Node: name=%s, cpus=%d, memory=%dM, state=%s>' % (self.name, self.cpus, self.memory, self.state)


class Job:

   def __init__(self):
      self.node = None
      self.partition = None
      self.priority = None
      self.user = None
      self.time = None
      self.time_str = None
      self.state = None
      self.id = None
      self.cpus = None
      self.requestedNodes = None
      self.qos = None
      self.memory = None
      
   def __repr__(self):
      return '<Job: id=%d, user=%s, state=%s, priority=%d, cpus=%d, time=%s, memory=%d>' \
          % (self.id, self.user, self.state, self.priority, self.cpus, self.time_str, self.memory)

   def cancel(self):
      scmd = ['scancel', '%d' % self.id]
      proc = subprocess.Popen(scmd, stdout=subprocess.PIPE)
      outStr = proc.communicate()[0]
      if len(outStr) > 0:
          print 'problem canceling job %d: %s' % (self.id, outStr)

   def update(self, params):
      plist = ['%s=%s' % (x[0], x[1]) for x in params.iteritems()]
      scmd = ['scontrol', 'update', 'JobId=%d' % self.id]
      scmd.extend(plist)

      proc = subprocess.Popen(scmd, stdout=subprocess.PIPE)
      outStr = proc.communicate()[0]
      if len(outStr.strip()) == 0:
         if 'QOS' in params.keys():
            self.qos = params['QOS']
         if 'Priority' in params.keys():
            self.priority = int(params['Priority'])   
         #obviously this code should be extended to support more job properties...

      else:
         print 'Problem setting params for job %d: %s' % (self.id, outStr)


   def set_qos(self, qos):
      scmd = ['scontrol', 'update', 'JobId=%d' % self.id, 'QOS=%s' % qos]
      proc = subprocess.Popen(scmd, stdout=subprocess.PIPE)
      outStr = proc.communicate()[0]
      if len(outStr.strip()) == 0:
         self.qos = qos
      else:
         print 'Problem setting QOS for job %d: %s' % (self.id, outStr)
      



def get_slurm_config():
   """ Obtain a Config object that contains lots of slurm
       information, including partitions, nodes, node states,
       and users
   """
   cfg = Config()

   #retrieve partitions and their nodes
   scmd = ['sinfo', '-h', '-o', '%P %N %T']

   proc = subprocess.Popen(scmd, stdout=subprocess.PIPE)
   outStr = proc.communicate()[0]

   lns = [ln.strip() for ln in outStr.split('\n')]

   for ln in lns:
      if len(ln) > 0:
         pdata = ln.split(' ')
         pName = pdata[0]
         if pName not in cfg.partitions:
            p = Partition()
            p.name = pName
            cfg.partitions[pName] = p
         p = cfg.partitions[pName]
         nstate = pdata[2]
         for nodeName in pdata[1].split(','):            
            if len(nodeName) > 0:
               if nodeName not in cfg.nodes:
                  n = get_node_info(nodeName)
                  cfg.nodes[n.name] = n
               if nodeName not in p.nodes:
                  p.nodes[nodeName] = nstate

   #get users
   scmd = ['sacctmgr', '-n', '-P', 'list', 'user']
   proc = subprocess.Popen(scmd, stdout=subprocess.PIPE)
   outStr = proc.communicate()[0]

   lns = [ln.strip() for ln in outStr.split('\n')]
   for ln in lns:
      if len(ln) > 0:
         uvals = ln.split('|')
         uname = uvals[0]
         if uname not in cfg.users:
            cfg.users.append(uname)

   return cfg


def get_node_info(nodeName):
   """ Get detailed information about a node, excluding job information """
   scmd = ['sinfo', '-h', '-N', '-n', nodeName, '-o', '%c %m %T']

   proc = subprocess.Popen(scmd, stdout=subprocess.PIPE)
   outStr = proc.communicate()[0]

   lns = [ln.strip() for ln in outStr.split('\n')]

   ndata = lns[0].split(' ')
   n = Node()
   n.name = nodeName
   n.cpus = int(ndata[0])
   n.memory = int(ndata[1])
   n.state = ndata[2].strip('*')
   
   return n



def parse_time(timeStr):
   """Parses an squeue-like time, returns time in seconds"""

   sleft = timeStr.split('-')

   ndays = 0
   nhours = 0
   nmins = 0
   nsecs = 0

   if len(sleft) > 1:
      ndays = int(sleft[0])
      sleft = sleft[1]
   else:
      sleft = sleft[0]
   
   sleft = sleft.split(':')
   if len(sleft) == 3:
      nhours = int(sleft[0])
      nmins = int(sleft[1])
      nsecs = int(sleft[2])
   elif len(sleft) == 2:
      nmins = int(sleft[0])
      nsecs = int(sleft[1])
   
   totalTime = ndays*86400 + nhours*3600 + nmins*60 + nsecs
   return totalTime


def get_job_info():
    """ Returns a list of Job objects """

    sqcmd = ['squeue', '-h', '-o', '%N|%P|%Q|%u|%M|%T|%i|%C|%n|%q|%m']

    proc = subprocess.Popen(sqcmd, stdout=subprocess.PIPE)
    outStr = proc.communicate()[0]
    lns = [l.strip() for l in string.split(outStr, '\n')]

    jobs = []
    for ln in lns:
        if len(ln) > 0:
            jstr = ln.split('|')
            j = Job()
            j.node = jstr[0]
            j.partition = jstr[1]
            j.priority = int(jstr[2])
            j.user = jstr[3]
            j.time_str = jstr[4]
            j.time = parse_time(jstr[4])
            j.state = jstr[5]
            j.id = int(jstr[6])
            j.cpus = int(jstr[7])
            j.requestedNodes = jstr[8]
            j.qos = jstr[9]
            j.memory = jstr[10]
            jobs.append(j)

    return jobs
