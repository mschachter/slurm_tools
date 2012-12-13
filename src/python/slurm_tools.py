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


class JobToRun:

    def __init__(self):
        self.cmds = None
        self.params = None
        self.completion_file = None
        self.running = False
        self.start_times = 0
        self.completed = False
        self.job_id = -1


class SlurmBot:

    def __init__(self):
        self.jobs = []
        self.poll_interval = 37.0
        self.max_jobs = 10


    def add(self, cmdStrs, sbatchParams, completion_fileName = None):
        sj = JobToRun()
        sj.cmds = cmdStrs
        sj.params = sbatchParams
        sj.completion_file = completion_fileName
        self.jobs.append(sj)


    def clear(self):
        del self.jobs[0:len(self.jobs)]


    def get_queued_jobs(self):
        return filter(lambda sj: not sj.completed and not(sj.running), self.jobs)


    def get_running_jobs(self):
        return filter(lambda sj: sj.running and not(sj.completed), self.jobs)


    def get_uncompleted_jobs(self):
        ujList = []
        for sj in self.jobs:
            if not sj.completed or not os.path.exists(sj.completion_file):
                ujList.append(sj)
        return ujList

    def mark_completed_jobs(self):
        for sj in self.jobs:
            if sj.completion_file != None and os.path.exists(sj.completion_file):
                sj.completed = True

    def update_running_jobs(self):

        jobInfo = slurm_squeue()
        for rj in self.get_running_jobs():
            jinfo = filter(lambda j: j['ID'] == rj.job_id, jobInfo)
            if len(jinfo) == 0:
                rj.running = False
                if rj.completion_file is None:
                    print 'Marking job %d as completed (no completion file specified)' % rj.job_id
                    rj.completed = True
                elif rj.completion_file != None and os.path.exists(rj.completion_file):
                    print 'Marking job %d as completed' % rj.job_id
                    rj.completed = True


    def run_and_wait(self, ignoreCompleted = False):

        if not ignoreCompleted:
            self.mark_completed_jobs()
            uj = self.get_uncompleted_jobs()
            nComp = len(self.jobs) - len(uj)
            print 'Marked %d jobs as already completed' % nComp

        self.update_running_jobs()
        queuedJobs = self.get_queued_jobs()
        runningJobs = self.get_running_jobs()

        while len(queuedJobs) > 0 or len(runningJobs) > 0:

            self.update_running_jobs()

            queuedJobs = self.get_queued_jobs()
            runningJobs = self.get_running_jobs()

            print '# of queued jobs: %d' % len(queuedJobs)
            print '# of running jobs: %d' % len(runningJobs)

            nJobsAvailable = min(len(queuedJobs), self.max_jobs - len(runningJobs))
            print '# of available job space to run: %d' % nJobsAvailable
            if nJobsAvailable > 0 and len(queuedJobs) > 0:

                for k in range(nJobsAvailable):
                    #Run jobs
                    sj = queuedJobs[k]
                    job_id = slurm_sbatch(sj.cmds, **sj.params)
                    sj.running = True
                    sj.job_id = job_id
                    if sj.start_times == 0:
                        print 'Started job with id %d' % job_id
                    else:
                        print 'Restarted job with id %d' % job_id
                    sj.start_times += 1

            time.sleep(self.poll_interval)


    def run(self):

        for sj in self.get_queued_jobs():
            cmdStrs = sj.cmds
            slurmParams = sj.params
            job_id = slurm_sbatch(cmdStrs, **slurmParams)
            sj.job_id = job_id
            print 'Started job with id %d' % job_id



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

def slurm_squeue(**keywords):

    sqcmd = ['squeue', '-h', '-o', '%N %P %Q %u %M %T %i %C']
    if 'username' in keywords:
        sqcmd.append('-u')
        sqcmd.append(keywords['username'])

    proc = subprocess.Popen(sqcmd, stdout=subprocess.PIPE)
    outStr = proc.communicate()[0]

    jobsInfo = []
    for l in string.split(outStr, '\n'):
        if len(l) > 0:
            jstr = string.split(l)
            if len(jstr) < 8:
                jstr.insert(0, 'None')
            jinfo = {}
            jinfo['NODELIST'] = jstr[0]
            jinfo['PARTITION'] = jstr[1]
            jinfo['PRIORITY'] = int(jstr[2])
            jinfo['USER'] = jstr[3]
            jinfo['TIME'] = jstr[4]
            jinfo['STATE'] = jstr[5]
            jinfo['ID'] = int(jstr[6])
            jinfo['CPUS'] = int(jstr[7])
            jobsInfo.append(jinfo)

    return jobsInfo


def slurm_sbatch(cmdList, **sbatchParams):
    """Run a command using sbatch, returning the job id.

    Keyword arguments:
    partition -- the name of the partition to run on
    depends -- a comma-separated list of job id dependencies
    out -- file path to write stdout to
    err -- file path to write stderr to
    nodes -- the # of nodes this job will require
    qos -- the quality-of-service for this job
    cpus -- the # of CPUs required by this job
    script -- the name of the script file that will be written, defaults to a temporary file

    Returns the job id.
    """

    sbatchCmds = []
    if 'partition' in sbatchParams:
        sbatchCmds.append('-p')
        sbatchCmds.append(sbatchParams['partition'])
    if 'depends' in sbatchParams:
        sbatchCmds.append('-d')
        sbatchCmds.append(sbatchParams['depends'])
    if 'out' in sbatchParams:
        sbatchCmds.append('-o')
        sbatchCmds.append(sbatchParams['out'])
    if 'err' in sbatchParams:
        sbatchCmds.append('-e')
        sbatchCmds.append(sbatchParams['err'])
    if 'nodes' in sbatchParams:
        sbatchCmds.append('-N')
        sbatchCmds.append('%d' % sbatchParams['nodes'])
    if 'qos' in sbatchParams:
        sbatchCmds.append('--qos=%d' % sbatchParams['qos'])
    if 'cpus' in sbatchParams:
        sbatchCmds.append('-c')
        sbatchCmds.append('%d' % sbatchParams['cpus'])
    if 'mem' in sbatchParams:
        sbatchCmds.append('--mem')
        sbatchCmds.append(sbatchParams['mem'])

    #Create a temp batch script
    if 'script' in sbatchParams:
        ofname = sbatchParams['script']
        ofd = open(ofname, 'w')
    else:
        (ofid, ofname) = tempfile.mkstemp(prefix='slurmtools_')
        ofd = open(ofname, 'w')

    #write the temp batch script
    ofd.write('#!/bin/sh\n')
    sbHeader = '#SBATCH %s\n' % ' '.join(sbatchCmds)
    ofd.write(sbHeader)
    ofd.write(' '.join(cmdList))
    ofd.write('\n')
    ofd.close()

    jobId = slurm_sbatch_from_file(ofname)
    return jobId


def slurm_sbatch_from_file(fileName):

    finalCmds = ['sbatch', '-v', fileName]

    #run the sbatch process
    proc = subprocess.Popen(finalCmds, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    jobId = -1
    attempts = 0
    maxAttempts = 100
    while attempts < maxAttempts:

        time.sleep(0.1)

        odata = proc.stdout.read()
        m = re.search('batch\\sjob\\s\\d*', odata)
        if m is not None:
            mstr = m.group()
            jobId = int(mstr.split()[2])
            break

        attempts += 1

    return jobId



def slurm_srun(cmdList, **srunParams):
    """Run a command using srun, returning the job id.

    Keyword arguments:
    partition -- the name of the partition to run on
    depends -- a comma-separated list of job id dependencies
    out -- file path to write stdout to
    err -- file path to write stderr to
    nodes -- the # of nodes this job will require
    qos -- the quality-of-service for this job
    """

    srunCmds = ['srun', '-v']
    if 'partition' in srunParams:
        srunCmds.append('-p')
        srunCmds.append(srunParams['partition'])
    if 'depends' in srunParams:
        srunCmds.append('-d')
        srunCmds.append(srunParams['depends'])
    if 'out' in srunParams:
        srunCmds.append('-o')
        srunCmds.append(srunParams['out'])
    if 'err' in srunParams:
        srunCmds.append('-e')
        srunCmds.append(srunParams['err'])
    if 'nodes' in srunParams:
        srunCmds.append('-N')
        srunCmds.append(srunParams['nodes'])
    if 'qos' in srunParams:
        srunCmds.append('--qos=%d' % srunParams['qos'])

    for c in cmdList:
        srunCmds.append(c)

    (ofid, ofname) = tempfile.mkstemp(prefix='slurmtools_')
    proc = subprocess.Popen(srunCmds, stdout=ofid, stderr=ofid)

    jobId = -1
    attempts = 0
    maxAttempts = 30
    while attempts < maxAttempts:

        time.sleep(0.5)

        f = open(ofname)
        odata = f.read()
        f.close()

        retCode = proc.poll()
        if retCode is not None:
            print 'srun may have failed, output below:'
            print odata
            return -1

        m = re.search('jobid\\s\\d*', odata)
        if m is not None:
            mstr = m.group()
            jobId = int(mstr.split()[1])
            break

        attempts += 1

    return jobId
