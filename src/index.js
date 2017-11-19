const fs = require('fs');
const zlib = require('zlib');
const cluster = require('cluster');
const numCPUs = require('os').cpus().length;
const EventEmitter = require('events');

module.exports = class ThreadFactory extends EventEmitter {
  constructor(spec) {
    super();
    this.spec = spec;
    this.end = false;
  }

  async start() {
    
    const spec = this.spec;
    let worker;
    
    if (cluster.isMaster) {
      const workers = {};
      let instructions = [];
      let job = 0;
      let i = 0;

      // run manager function to get a list of instructions for the workers
      try {
        instructions = await spec.manager();
      } catch (e) {
        throw e;
      }
  
      // turn the insutrctions into a rob list
      const jobList = instructions.map((row) => (
        {
          started: null,
          finished: null,
          startedBy: null,
          result: null,
          instruction: row,
        }
      ));

      const createResume = (resume) => {
        if (i >= jobList.length) {
          // this.emit('finsihed', jobList);
          const left = jobList.filter((res) => {
            if (res.finished === null) {
              return true;
            }
          });
          if (left.length === 0 && this.end === false) {
            this.end = true;
            this.emit('finished', jobList);
          }
          return;
        }

        // fill in resume with new job
        resume.job = jobList[i];
        resume.index = i;
        resume.status = 'job';

        // fill in the job application
        jobList[i].started = new Date();
        jobList[i].startedBy = resume.id;
        i++;

        // send job to worker
        cluster.workers[resume.id].send(resume);
      };

      const handleMessage = (resume) => {
        if (resume.status === 'finished') {
          this.emit('completed', resume);
          // complete the job
          jobList[resume.index].finished = new Date();
          jobList[resume.index].result = resume.result;
          createResume(resume);
        } else if (resume.status === 'ready') {
          createResume(resume);
        } else if (resume.status === 'error') {
          this.emit('error', resume.result);
        }
      };
    
      // spin up extra processes
      for (let i = 0; i < numCPUs; i++) {
        cluster.fork();
      }

      console.log(`started ${numCPUs} processes`);
    
      // send processes constructor data
      for (const id in cluster.workers) {
        // create listener
        cluster.workers[id].on('message', handleMessage);
    
        // send meta data to fork
          const resume = {
            id,
            status: 'clockin',
          }
    
          // supply forks with meta data
          cluster.workers[id].send(resume);
          workers[id] = resume;
      }
    } else {
      process.on('message', async (resume) => {
        if (resume.status === 'clockin') {
          // a new worker is checking in to work,
          // this is basically a constructor for the process.
          worker = resume;
          try {
            await spec.workerStartup(worker);
            worker.status = 'ready';
          } catch (e) {
            worker.status = 'error';
            worker.result = e;
          }
          // tell the manager you are ready to work
          process.send(worker);
  
        } else if (resume.status === 'job') {
          // worker has received a new job
          const draftResume = resume;
          try {
            draftResume.status = 'working';
            draftResume.result = await spec.workerProcess(draftResume.job.instruction, draftResume);
            draftResume.status = 'finished';
          } catch (e) {
            draftResume.result = e;
            draftResume.status = 'error';
          }
          // send the result back to the manager
          process.send(draftResume);
        }
      });
    }
  }
}