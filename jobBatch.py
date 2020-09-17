#!/usr/bin/env python

""""
Title:          Batch processing with bonus hours on Raijin
Author:         James Beattie
First Created:  04 / 08 / 2019

"""

from os import system
import subprocess
import argparse

# Command line arguments
##########################################################################################################

ap		= argparse.ArgumentParser(description = 'Input arguments')
ap.add_argument('-nJobs',required=True,default=None,help='the number of files to write',type=int)
ap.add_argument('-nCores',required=True,default=None,help='the number of cores requested for each job',type=int)
ap.add_argument('-flash',required=True,default=None,help='the compiled flash file',type=str)
args	= vars(ap.parse_args())

# Command line examples
##########################################################################################################
"""
jobBatch.py -nJobs 24 -nCores 4096 -flash "flash4"

for running 24 jobs, using 4096 compute cores and using the compiled FLASH executable 'flash4'

"""

# Function definitions
##########################################################################################################

def makeJobFile(jobFileNum,nCores,flashFile,email,jobDepend=None):
    """
    Create a new job file

    INPUTS:
    ########################################################
    jobFileNum  : the number of the job file
    flashFile   : the name of the compiled flash file
    jobDepend   : the dependency for the job

    OUTPUTS:
    ########################################################
    A written job file.
    """

    jobFile = "job{}.sh".format(jobFileNum)
    print("\n--------------------------------------------------------")
    print("Building Job File {}.".format(jobFileNum))
    print("--------------------------------------------------------\n")

    system("touch " + jobFile)
    job = open(jobFile,"w")
    print("Writing job file: job{}.sh".format(jobFileNum))
    job.write("#!/bin/bash \n")
    job.write("#PBS -P ek9 \n")
    job.write("#PBS -q normal \n")
    job.write("#PBS -l walltime=05:00:00 \n")
    job.write("#PBS -l ncpus={} \n".format(nCores))
    job.write("#PBS -l mem={}".format(nCores*4) +"GB \n")
    job.write("#PBS -l wd \n")
    job.write("#PBS -N job{} \n".format(jobFileNum))
    job.write("#PBS -j oe \n")
    job.write("#PBS -m bea \n")
    job.write("#PBS -l storage=scratch/ek9+gdata/ek9")
    if jobDepend is not None:
        job.write("#PBS -W depend=afterok:{} \n".format(jobDepend))
    job.write("#PBS -M {} \n \n".format(email))
    if jobDepend is not None:
        print("Writing a prep_restart.py in job file: job{}.sh".format(jobFileNum))
        job.write("prep_restart.py -auto 1>shell_res.out{} 2>&1 \n".format(jobFileNum))
    job.write("mpirun -np $PBS_NCPUS -x UCX_TLS=rc ./{} 1>shell.out0{} 2>&1".format(flashFile,jobFileNum))
    job.close()

def submitJobFiles(numOfFiles,nCores,flashFile):
    """
    Write and submit job files.

    INPUTS:
    ########################################################
    numOfFiles  : the number of job files you want to submit.
    flashFile   : the name of the compiled flash file.

    OUPUTS:
    ########################################################
    numOfFiles number of job files submitted to Raijin.
    """

    print("\n--------------------------------------------------------")
    print("Beginning to write and submit job files.")
    print("--------------------------------------------------------\n")

    jobDependacy = None
    
    
    email = raw_input("What email do you want to send job updates to: \n")
    print("You are about to submit {}".format(numOfFiles) + " files.")
    print("You are using the flash file: {}".format(flashFile))
    print("This will be {}".format(numOfFiles*4) + " walltime hours in total.")
    cont   = raw_input("Do you want to continue (y/n)? \n")
    while cont not in ["y","n"]:
        cont = raw_input("Please only input (y/n).")
    if cont == "n":
        return
    
    # Loop through jobs.
    for jobFileNum in xrange(1,numOfFiles + 1):
        makeJobFile(jobFileNum,nCores,flashFile,email,jobDependacy)
        print("Submitting job file: job{}.sh".format(jobFileNum))
        proc            = subprocess.Popen(["qsub job{}.sh".format(jobFileNum)], stdout=subprocess.PIPE, shell=True)
        (out, err)      = proc.communicate()
        print "The std output is {}".format(out)
        jobDependacy    = out.split('.')[0]
        if jobDependacy is None:
            print("Breaking out of the job file submission process because std out is None")
            break
        print("Job file number : {}".format(jobDependacy))

# Main
#######################################################################################################
if __name__ == "__main__":
    submitJobFiles(args['nJobs'],args['nCores'],args['flash'])
