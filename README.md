# NCI Dependent Batch Processing
Batch processing with job dependencies on NCI, using Python 2.7. 
- `jobBatch.py` will create a batch of `n` jobs, where the `n+1`th job depends upon the execution of the `n`th job. 

## Dependencies
- `subprocess` module
- `argparse` module
- `prep_restart.py` executable in `$USER/bin` (ask Christoph for this script)

## Useage
- Make `jobBatch.py` an executable in `$USER/bin`,  as well as `prep_restart.py`.
- Update the `makeJobFile()` function in `jobBatch.py` to match the job file that you want to batch process.
- Use the command line to run `jobBatch.py` in the directory with your FLASH simulation.

## Command Line Examples
```
jobBatch.py -nJobs 24 -nCores 4096 -flash "flash4"
```
creates 24 jobs, each requesting 4096 compute cores, using the compiled FLASH executable `flash4`. After the first job, the `prep_restart.py` script as run inbetween jobs.
