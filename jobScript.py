#!/usr/bin/env python

""""
Title:          job.sh generation script
Author:         James Beattie. Some mods by Shyam Menon (2021)
"""

import os
import argparse
import subprocess
#Available job queues in Gadi
nci_jobQueues = ["normal","express","hugemem","megamem","gpuvolta","normalbw","expressbw",
                "normalsl","hugemembw","megamembw","copyq"]
#Charge rates in SU
nci_chargeRates = [2,6,3,5,3,1.25,3.75,1.5,1.25,1.25,2]

def check_Limits(wallTime=1,nCpu=48,jobMemory=None,jobQueue='normal',verbose=False):
    #Limit checking only for these queues for now.
    if(jobQueue not in ['normal','express']):
        return
    if(jobMemory is None):
        jobMemory = 4*nCpu

    if(jobMemory/nCpu > 4):
        raise ValueError("Memory per core requested greater than 4 GB.")

    #Normal Queue limits
    if(jobQueue == 'normal'):
        if(nCpu<=672):
            wallTimeLimit = 48
        elif(nCpu>672 and nCpu<=1440):
            wallTimeLimit = 24
        elif(nCpu>1440 and nCpu<=2976):
            wallTimeLimit = 10
        elif(nCpu>20736):
            raise ValueError("nCPUS requested greater than normal max request.")
        else:
            wallTimeLimit = 5

    #Express Queue limits
    if(jobQueue == 'express'):
        if(nCpu<=480):
            wallTimeLimit = 24
        elif(nCpu>480 and nCpu<=3168):
            wallTimeLimit = 5
        else:
            raise ValueError("Too many cores requested for express queue.")

    if(wallTime > wallTimeLimit):
        raise ValueError("Walltime requested too high. Limit for {} queue with {} cores is {}".format(jobQueue,nCpu,wallTimeLimit))

    return

def computeJobCharge(wallTime=1,nCpu=48,jobMemory=None,jobQueue='normal'):
    #Get chargeRate
    chargeRate = nci_chargeRates[nci_jobQueues.index(jobQueue)]
    #Compute job cost
    jobCost = chargeRate*max(jobMemory/4.0,nCpu)*wallTime
    return jobCost

def getjobChargeUnit(jobCharge):
    if(jobCharge<=1000):
        jobChargeUnit = 'SU'
    elif(jobCharge>1000 and jobCharge<1.e6):
        jobCharge = jobCharge/1.e3
        jobChargeUnit = 'KSU'
    else:
        jobCharge = jobCharge/1.e6
        jobChargeUnit = 'MSU'
    return jobCharge,jobChargeUnit

def submitJob(jobFile='job.sh',verbose=False):
    
    if(not os.path.isfile(jobFile)):
        raise ValueError("Provided jobFile {} does not exist.".format(jobFile))
    if(verbose):
        print("Submitting {}".format(jobFile))

    proc = subprocess.Popen(["qsub {}".format(jobFile)], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        shell=True)
    #Clean exit on timeout (see subprocess documentation)
    try:
        (out, err) = proc.communicate(timeout=30)
    except TimeoutExpired:
        proc.kill()
        (out,err) = proc.communicate()

    out = out.decode('utf-8')
    err = err.decode('utf-8')
    jobID = out.split('.')[0]
    if(not jobID):
        print("Failed attempt at submitting script with command: qsub {}".format(jobFile))
        print("STDOUT: {}".format(out))
        print("STDERR: {}".format(err))
        raise OSError("Exiting.")
    if(verbose):
        print(out)

    print("Submitted {}. JobFileNumber: {}".format(jobFile,jobID))
    return jobID


def makeJobFile(jobFile='job.sh',nCpu=48,jobName='FlashSim',wallTime=1,jobMemory=None,
    jobQueue='normal',flashExec='flash4',email=None,verbose=False):
    """
    Create a new job file
    OUTPUTS:
    ########################################################
    A job.sh file template.
    """
    #Automatic memory if not provided
    if(jobMemory is None):
        jobMemory = nCpu*4

    #First safety check for walltime/memory limits
    check_Limits(wallTime,nCpu,jobMemory,jobQueue,verbose=verbose)

    #Email
    if(email is None):
        try:
            email = os.environ["EMAIL"]
        except KeyError:
            email = None

    os.system("touch {}".format(jobFile))
    job = open(jobFile,"w")
    if(verbose):
        print("Creating job file for job {} using {} cores and {} GB memory in the {} queue for {} hours.".format(jobName,
            nCpu,jobMemory,jobQueue,wallTime))
        print("Writing job file: {}".format(jobFile))
    job.write("#!/bin/bash \n")
    job.write("#PBS -P ek9 \n")
    job.write("#PBS -q {} \n".format(jobQueue.lower()))
    job.write("#PBS -l walltime={:02d}:00:00 \n".format(int(wallTime)))
    job.write("#PBS -l ncpus={}\n".format(int(nCpu)))
    job.write("#PBS -l mem={}GB\n".format(int(jobMemory)))
    job.write("#PBS -l wd \n")
    job.write("#PBS -N {} \n".format(jobName))
    job.write("#PBS -j oe \n")
    if(email is not None):
        job.write("#PBS -m bea \n")
        job.write("#PBS -M {} \n".format(email.strip()))
    job.write("#PBS -l storage=scratch/ek9+gdata/ek9 \n")
    
    job.write("mpirun -np $PBS_NCPUS -x UCX_TLS=rc ./flash4 1>shell.out 2>&1")
    job.close()

    if(verbose):
        print("Job file {} created.".format(jobFile))

    #Compute Cost of Job
    jobCharge = computeJobCharge(wallTime,nCpu,jobMemory,jobQueue)
    if(verbose):
        jobChargeVal, jobChargeUnit = getjobChargeUnit(jobCharge)
        print("Cost of Job is : {} {}".format(jobChargeVal,jobChargeUnit))
    return jobFile,jobCharge

if __name__ == "__main__":
    
    ap      = argparse.ArgumentParser(description = 'Input arguments')
    ap.add_argument('-file','-f',dest='jobFile',default='job.sh',help='the name of the job script',type=str)
    ap.add_argument('-nCores','-ncpu','--ncpu','-np',dest='nCpu',default=48,help='the number of cores requested for each job',type=int)
    ap.add_argument('-jobname','-j','-N',dest='jobName',default='FlashSim',help='the name of the submitted job',type=str)
    ap.add_argument('-time','-t','-w',dest='wallTime',default=1,help='the walltime limit for the job in hours',type=int)
    ap.add_argument('-memory','-mem',dest='jobMemory',default=None,help='the memory required for the job in GB',type=int)
    ap.add_argument('-email','-e',dest='email',default=None,help='the email ID to send updates to.',type=int)
    ap.add_argument('-queue','-q',dest='jobQueue',default='normal',help='the queue to submit to',type=str)
    ap.add_argument('-flash',dest='flashExec',default='flash4',help='the compiled flash file',type=str)
    ap.add_argument("-submit", "--submit", dest='submit',action='store_true', 
        help="Submit script after creating.")
    ap.add_argument("-verbose", "--verbose", dest='verbose',action='store_true', 
        help="More printing messages for debugging.")
    args = ap.parse_args()

    jobFile,jobCharge = makeJobFile(jobFile=args.jobFile,nCpu=args.nCpu,jobName=args.jobName,
        wallTime=args.wallTime,jobMemory=args.jobMemory,jobQueue=args.jobQueue,flashExec=args.flashExec,
        email=args.email,verbose=args.verbose)
    if(args.submit):
        jobID = submitJob(args.jobFile,args.verbose)
    
    #Final Print

    jobChargeVal, jobChargeUnit = getjobChargeUnit(jobCharge)
    print("Expected Cost: {} {}".format(jobChargeVal,jobChargeUnit))
    if(args.submit):
        print("JobID: {}".format(jobID))