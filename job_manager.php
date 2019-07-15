<?php /** @noinspection PhpUndefinedMethodInspection */

/*
mysql

create table job(
   id bigint auto_increment primary key,
   parameters text,
   val text,
   name varchar(255),
   hash varchar(255),
   is_done boolean default false,
   is_failed boolean default false,
   last_error text,
   last_step_finished_at datetime,
   first_step_started_at datetime,
   try_count int default 0
)
create unique index job_name_hash on job(name,hash);


create table job_step(
   id bigint auto_increment primary key,
   job_id bigint not null references job(id),
   pos int not null,
   subpos int,
   is_failed boolean default false,
   run_once boolean default false,
   run_after datetime,
   try_count int
)

create table job_step_depends_on(
  job_step_id bigint not null, -- references job_step(id),
  depends_on_step_id bigint not null, -- references job_step(id),
  primary key(job_step_id, depends_on_step_id)
)

delimiter //
drop procedure if exists jobs.execute//
create procedure jobs.execute(name varchar(128), param text, run_after datetime)
begin
  declare iid bigint;
  declare all_param text default concat('{"name":"',name,'", "param":',param,'}');
  declare exit handler for sqlexception begin
   rollback work to savepoint execute_svp;
   resignal;
  end;
  savepoint execute_svp;
  insert into jobs.job(name,parameters,hash) values('.execute',all_param, md5(all_param));
  set iid = last_insert_id();
  insert into jobs.job_step(job_id,pos,run_after) values(iid,0,coalesce(run_after,now()));
  release savepoint execute_svp;
end;
//



Postgres
CREATE TABLE public.job
(
  id bigint NOT NULL DEFAULT nextval('job_id_seq'::regclass),
  parameters text,
  val text,
  name character varying(255),
  hash character varying(255) unique,
  is_done boolean DEFAULT false,
  is_failed boolean DEFAULT false,
  last_error text,
  last_step_finished_at timestamp without time zone,
  first_step_started_at timestamp without time zone,
  try_count int default 0,
  CONSTRAINT job_pkey PRIMARY KEY (id)
);
create unique index job_name_hash on job(name,hash);
CREATE TABLE public.job_step
(
  id bigint NOT NULL DEFAULT nextval('job_step_id_seq'::regclass),
  job_id bigint NOT NULL,
  pos integer NOT NULL,
  subpos integer,
  is_failed boolean DEFAULT false,
  run_once boolean default false,
  run_after timestamp,
  CONSTRAINT job_step_pkey PRIMARY KEY (id),
  CONSTRAINT job_step_job_id_fkey FOREIGN KEY (job_id)
      REFERENCES public.job (id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
);
CREATE TABLE public.job_step_depends_on
(
  job_step_id bigint NOT NULL,
  depends_on_step_id bigint NOT NULL,
  CONSTRAINT job_step_depends_on_pkey PRIMARY KEY (job_step_id, depends_on_step_id)
 --   CONSTRAINT job_step_depends_on_depends_on_step_id_fkey FOREIGN KEY (depends_on_step_id)
 --      REFERENCES public.job_step (id) MATCH SIMPLE
 --      ON UPDATE NO ACTION ON DELETE NO ACTION,
 --  CONSTRAINT job_step_depends_on_job_step_id_fkey FOREIGN KEY (job_step_id)
 --      REFERENCES public.job_step (id) MATCH SIMPLE
 --      ON UPDATE NO ACTION ON DELETE NO ACTION
);
CREATE OR REPLACE FUNCTION jobs.delete_job(jid bigint)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
begin
  perform from jobs.job where id=jid for update;
  delete from jobs.job_step_depends_on where exists(select * from jobs.job_step js where js.id=job_step_depends_on.job_step_id and js.job_id=jid);
  delete from jobs.job_step_depends_on where exists(select * from jobs.job_step js where js.id=job_step_depends_on.depends_on_step_id and js.job_id=jid);
  delete from jobs.job_step where job_id=jid;
  delete from jobs.job where id=jid;
end;
$function$

CREATE OR REPLACE FUNCTION jobs.resume_job(jid bigint)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
begin
   update jobs.job_step set is_failed=false, try_count=case when try_count is null then null else 1 end where job_id=jid;
   update jobs.job set is_failed=false where id=jid;
end;
$function$

*/
    class JobSubmitException extends \Exception{};
    class JobExecuteException extends \Exception{};
    class JobRunException extends \Exception{};
    class JobTxnLevelUnderflow extends \Exception{};
    class JobStressTestException extends \Exception{};
    class JobSqlException extends \Exception{};
    class JobDeleteException extends \Exception{};
    class JobLogger{
       /** @var int */
       const PANIC = 0;
       /** @var int */
       const ERROR = 1;
       /** @var int */
       const WARN  = 2;
       /** @var int */
       const WARNING = 2;
       /** @var int */
       const INFO = 3;
       /** @var int */
       const DEBUG = 4;
       /** @var int */
       const TRACE = 5;

       /** @var string */
       private $lastLogFileName = null;
       /** @var resource */
       private $logFile = null;
       /** @var int */
       private $level;

       function __construct(int $ll){
            $this->level = $ll;
       }

       function getLogLevel(): int{
            return $this->level;
       }
       /** @return void */
       function openFile(){
            $logFileName = (getenv('JOB_MANAGER_LOGPREFIX')?:'job-runner-') . date('Y-m-d') . '.log';
            if($this->lastLogFileName == $logFileName){
                return;
            }
            if($this->logFile){
                if(!fclose($this->logFile)){
                    error_log("Cannot close old log file {$this->lastLogFileName}");
                }
            }
            $this->logFile = fopen(getenv("JOB_MANAGER_LOGDIR") . './' . $logFileName, "a");
            if(!$this->logFile){
                error_log("Cannot open log file " . getenv("JOB_MANAGER_LOGDIR") . './' . $logFileName);
            }
            $this->lastLogFileName = $logFileName;
       }

        /**
         * @param string $str
         * @return void
         */
       function writeToLog(string $str){
            $str = preg_replace("/\r?\n|\n\r?/",'\\n',$str);
            if($this->logFile)
                fwrite($this->logFile, "$str\n");
            else{
                error_log("Log destination file is not opened; error message is $str");
            }
       }

        /**
         * @param string $level
         * @param string $msg
         * @param array<string>|null $opt
         * @return void
         */
       function printLine(string $level, string $msg, array $opt = null){
            $this->openFile();
            $msg = vsprintf($msg, $opt); // @phan-suppress-current-line PhanPluginPrintfVariableFormatString,PhanTypeMismatchArgumentInternal
            $outString = 'job-runner [' . date('Y-m-d H-i-s') . "] $level: $msg";
            $this->writeToLog($outString);
       }

        /**
         * @param string $msg
         * @param array|null $opt
         * @return void
         */
       function panic(string $msg, array $opt = null){
            if($this->level <= JobLogger::PANIC)
                return;
            $this->printLine('PANIC', $msg, $opt);
       }

        /**
         * @param string $msg
         * @param array|null $opt
         * @return void
         */
       function error(string $msg, array $opt = null){
            if($this->level <= JobLogger::ERROR)
                return;
            $this->printLine('ERROR', $msg, $opt);
       }

        /**
         * @param string $msg
         * @param array|null $opt
         * @return void
         */
       function warn(string $msg, array $opt = null){
            if($this->level <= JobLogger::WARN)
                return;
            $this->printLine('WARN', $msg, $opt);
       }

        /**
         * @param string $msg
         * @param array|null $opt
         * @return void
         */
       function info(string $msg, array $opt = null){
            if($this->level <= JobLogger::INFO)
                return;
            $this->printLine('INFO', $msg, $opt);
       }

        /**
         * @param string $msg
         * @param array|null $opt
         * @return void
         */
       function debug(string $msg, array $opt = null){
            if($this->level <= JobLogger::DEBUG)
                return;
            $this->printLine('DEBUG', $msg, $opt);
       }
       /**
         * @param string $msg
         * @param ?array<mixed> $opt
         * @return void
         */
       function trace($msg, $opt = null){
            if($this->level <= JobLogger::TRACE)
                return;
            $this->printLine('TRACE', $msg, $opt);
       }

    }

    class Job{
        private $steps = [];
        private $runOnce = [];
        /** @var string */
        private $name;
        /** @var object $log */
        private $log;
        /** @var callable(array<mixed,mixed>,array<mixed,mixed|array>,object):void */
        private $cb;

        /**
         * @param string $name - name of submitted job
         * @param ?object= $log - log object, it must supports methods trace,debug,info,warn,error,panic
        */
        function __construct($name, $log=null){
            $this->name = $name;
            $this->log = $log ?: new JobLogger(getenv("JOB_MANAGER_LOGLVL") ?: JobLogger::INFO);
            $this->log->debug(" <{$this->name}> Create new job with name $name");
        }

        /**
         * @return string name of the job
         */
        function getName(): string{
            return $this->name;
        }

        /**
         * @return array<callable(array,array,JobExecutor):(mixed|object|array)|array<callable(array,array,JobExecutor):(mixed|object|array)>> - array of job's steps
         */
        function getSteps(): array{
            return $this->steps;
        }

        /**
         * @return array<mixed> - is the job should run only once (not tested)
         */
        function getRunOnce(): array{
            return $this->runOnce;
        }

        /**
         * submit job for one execution, i.e. no retites can be executed
         * @param array<mixed> $param
         * @return Job
         * @throws JobSubmitException
         */
        function submitOnce($param){
            return $this->submit($param, true);
        }

        /**
         * Set callback to be executed before and after each step
         * Callback will be called before as
         *                  ($je, $job, $param+['.state'=>'before'], $ctx);
         * and after as
         *                  ($je, $job, $param+['.state'=>'after'], $ctx,$result);
         * where
         *      $je - job executor object
         *      $job - job itself
         *      $param - parameters of the job
         *      $ctx - context of the job
         *      $result - it is result value of job step or Exception in case when job step will throw exception
         *
         * @param callable(array<mixed>,array<mixed>,object):void $fn
         * @return void
         * @throws JobSubmitException
         */
        function setCallback($fn){
            $this->setCb($fn);
        }

        /**
         * @see setCallback
         * @param callable(array,array,object):void $fn
         * @return void
         * @throws JobSubmitException
         */
        function setCb($fn){
            if(!is_callable($fn)){
                throw new JobSubmitException("Passed param is not function");
            }
            $this->cb = $fn;
        }
        /**
         * @see setCallback
         * @return callable(array,array,object):void $fn
         */
        function getCb(){
            return $this->cb;
        }
        /**
         * Add step to job to be executed
         * Steps executed sequently, one by one
         * Each step is callback or array of callbacks
         * This callback(s) will be called with parameters
         *      $param - array of parameters passed to callback
         *      &$ctx - step context, step can modify it and the modifications will be saved and passed to anothers
         *              steps or another job's steps if these steps depends on the step
         *      $je - job executor object, @see JobExecutor
         * @param array{} $param
         * @param callable(array{},array{}&,JobExecutor):(mixed|array|object) $param
         * @return Job
         * @throws JobSubmitException
         */
        function submit($param, $isOnce=0){
            $this->log->debug(" <{$this->name}> Submit new steps; isOnce=$isOnce");
            if(is_callable($param)){
                $this->log->debug(" <{$this->name}> add one step");
                $this->steps[] = $param;
                $this->runOnce[] = $isOnce;
            }elseif(is_array($param)){
                $this->log->debug(" <{$this->name}> add array of steps");
                foreach($param as $p){
                    if(!is_callable($p)){
                        $this->log->error(" <{$this->name}> found not callable reference in passed array of steps");
                        throw new JobSubmitException("Passed array has non-function element");
                    }
                }
                $this->steps[] = $param;
                $this->runOnce[] = $isOnce;
            }else{
                $this->log->error(" <{$this->name}> Passed param neither function nor array of functions");
                throw new JobSubmitException("Passed param neither function nor array of functions");
            }
            return $this;
        }

        /** @return void */
        function end(){
            return;
        }
    }

/**
 * Class JobExecutor
 * Executes submitted jobs, also offers some support for database access
 */
class JobExecutor extends sqlHelper{
    /**
     * Return values for job steps
     * DELETE - all job steps will be deleted and execution will be stopped
     * DONE - all job will be considerated as done
     * CONTINUE - execute same step again
     * null|false|0 - execution of the step is done, next step will be executed
     * positive number - same step will be executed after number of seconds
     * negative number - job will be considerated as failed
     * Throwable - job will be stopped with getMessage() of the exception as last_error
     *
     * If job throws an exception then job will be marked as failed and job execution will be stopped
     */
    /** @var string */
    const DELETE_CURRENT_JOB = 'DELETE';
    public static function DELETE_CURRENT_JOB(): string{
      return JobExecutor::DELETE_CURRENT_JOB;
    }
    /** @var string */
    const CURRENT_JOB_DONE = 'DONE';
    public static function CURRENT_JOB_DONE(): string{
      return JobExecutor::CURRENT_JOB_DONE;
    }
    public static function END_CURRENT_JOB(): string{
      return JobExecutor::CURRENT_JOB_DONE;
    }
    /** @var string */
    const CONTINUE_CURRENT_JOB = 'CONTINUE';
    public static function CONTINUE_CURRENT_JOB(): string{
      return JobExecutor::CONTINUE_CURRENT_JOB;
    }

    /** @var array<Job|array<Job>> */
    private $jobs=[];
    /** @var string */
    private $tp="";
    /** @var object */
    private $log;
    /** @var int */
    private $currentJobId;
    /** @var int */
    private $currentStepId;

    /**
     * @param int $jid
     * @return void
     */
    function setCurrentJobId($jid){
       $this->currentJobId = $jid;
    }
    /**
     * @return int
     */
    function getCurrentJobId(){
       return $this->currentJobId;
    }
    /**
     * @param int $sid
     * @return void
     */
    function setCurrentStepId($sid){
      $this->currentStepId = $sid;
    }
    /**
     * @return int
     */
    function getCurrentStepId(){
      return $this->currentStepId;
    }

    /**
     * @return string
     */
    function getJobExecutorGlobalName(){
      return getenv("JOB_MANAGER_LOGLVL") ?? gethostname();
    }


    /**
     * Creates job executor object
     * @param object $dbh - database connection to work on
     * @param string $tp - prefix for database objects, for example, 'jobs.'
     * @param ?object= $logger - logger object to use instead of default logger
     */
    function __construct($dbh=null, $tp = "", $logger=null){
         $this->log = $logger ?: new JobLogger(getenv("JOB_MANAGER_LOGLVL") ?: JobLogger::TRACE);
         $this->tp = $tp;
         if($dbh)
            $this->setDbh($dbh);

         $job = new Job('.execute', $this->log);
         /**
          * @param array{name:string,param:array,ctx?:array,delay?:int} $param
          * @param array<string,mixed|array|object> $ctx
          * @param JobExecutor $je
          * @return null
          * @throws JobSubmitException
          */
         $job->submit(function(array $param, array &$ctx, JobExecutor $je){
             try{
                 $je->execute(@$param['name'], @$param['param'], @$param['ctx'], @$param['delay']);
             }catch(\Exception $e){
                 $this->log->error("Internal executor: Cannot submit job {$param['name']} : {$e->getMessage()}");
             }
             return null;
         });
         $this->add($job);
    }

    /**
     * @param Job $job
     * @return void
     * @throws JobSubmitException
     */
    function add(Job $job){
        $this->log->debug("JobExecutor: add new job {$job->getName()}");
        $arr = $job->getSteps();
        if(is_array($arr[count($arr)-1]))
            $job->submit(function(){}); // @phan-suppress-current-line PhanTypeMismatchArgument
        $this->jobs[$job->getName()] = $job;
    }

    /**
     * @param string $jobName - Job name
     * @param array<string,mixed|array|object|null> $param - params for the job
     * @param array<string,mixed|array|object|null>|null= $ctx - execution context
     * @param int= delay - delay to start in seconds
     * @param array<int>= $depends_on - steps to be completed before submittted job will be executed
     * @param array<int>= $dependants - steps to wait this job successful completion
     * @return int
     * @throws JobExecuteException
     * @throws JobSubmitException
     * @throws JobTxnLevelUnderflow
     */
    function execute($jobName, $param, $ctx = [], $delay = 0, $depends_on = [], $dependants = []): int{
      $param =[ '.parent_id' => $this->getCurrentJobId(), '.parent_step_id' => $this->getCurrentStepId() ] + $param;
      $logPrefix = 'JobExecutor#execute[pid=' . getmypid() . ']';
      $jobStepStartTs = date('Y-m-d H:i:s', time()+$delay);

      $this->log->trace(" $logPrefix <$jobName> Submitted $jobName for execution at $jobStepStartTs");

      if(!is_string($jobName)){
         $this->log->error(" $logPrefix <$jobName> Passed jobName $jobName is not a string");
         throw new JobExecuteException("Passed jobName is not a string");
      }
      if(!is_array($param)){
         $this->log->error(" $logPrefix <$jobName> Passed \$param for jobName $jobName is not an array");
         throw new JobExecuteException("Passed \$param for jobName $jobName is not an array");
      }

      if($ctx && !is_array($ctx)){
         $this->log->error(" $logPrefix <$jobName> Passed \$ctx for jobName $jobName neither null nor array");
         throw new JobExecuteException("Passed ctx is not a string");
      }
      if($depends_on && !array_reduce($depends_on, function(int $a, int $b): bool { return $a && is_int($b); }, true)){
         throw new JobExecuteException("Passed \$depends_on is not array of ints");
      }

      if(!array_reduce($dependants, function(int $a, int $b): bool { return $a && is_int($b); }, true)){
         throw new JobExecuteException("Passed \$depends_on is not array of ints");
      }

      ksort($param);
      $tp = $this->tp;
      $hash = md5(serialize($param));
      $this->log->debug(" $logPrefix <$jobName> params hash is $hash");

      $job = $this->jobs[$jobName];

      if(!$job){
         $this->log->error(" $logPrefix <$jobName> Cannot find job $jobName between before supplied jobs");
         throw new JobExecuteException("Cannot find job $jobName");
      }

      try{
          $this->log->trace(" $logPrefix <$jobName> Going to insert steps for job");
          $this->setSavepoint();
            $this->log->trace(" $logPrefix <$jobName> Transaction started");

            if($this->dbDriver == 'pgsql'){
                $rc = $this->exec_query("insert into {$tp}job(parameters, val, name, hash ) values(?,?,?,?) on conflict(name,hash) do nothing", json_encode($param), json_encode($ctx), $jobName, $hash);
            }else{
                $rc = $this->exec_query("insert into {$tp}job(parameters, val, name, hash ) values(?,?,?,?) on duplicate key update hash=hash", json_encode($param), json_encode($ctx), $jobName, $hash);
            }
            if(!$rc){
                 $this->releaseSavepoint();
                 $this->log->debug(" $logPrefix <$jobName> Job $jobName with parameters hash equals to $hash already has been executed. Returning");
                 throw new JobExecuteException("<$jobName> Job $jobName with parameters hash equals to $hash already has been executed. Returning");
            }
            $jobId = $this->lastInsertId("{$tp}job_id_seq");
            $this->log->debug(" $logPrefix <$jobName> row inserted with id=$jobId");
            $ids=[]; $prevIds=$depends_on;
            $pos=0;

            foreach($job->getSteps() as $s){
              if(is_callable($s)){
                  $this->log->trace(" $logPrefix <$jobName> Insert single step");
                  $this->exec_query("insert into {$tp}job_step(job_id, pos, subpos, run_after, run_once, try_count) values(?,?,null,?,?,?)", $jobId, $pos, $jobStepStartTs, $job->getRunOnce()[$pos], $job->getRunOnce()[$pos]?:null);
                  $cid = $this->lastInsertId("{$tp}job_step_id_seq");
                  $this->log->trace(" $logPrefix <$jobName> Inserted single step id is $cid");
                  $ids = [ $cid ];
                  foreach($prevIds as $pid){
                    $this->exec_query("insert into {$tp}job_step_depends_on(job_step_id, depends_on_step_id) values(?,?)", $cid, $pid);
                  }
                  $this->log->trace(" $logPrefix <$jobName> Dependency rows have been inserted");
              }elseif(is_array($s)){
                  $subpos=0;
                  foreach($s as $s1){
                     $this->exec_query("insert into {$tp}job_step(job_id, pos, subpos, run_after, run_once, try_count) values(?,?,?,?,?,?)", $jobId, $pos, $subpos, $jobStepStartTs, $job->getRunOnce()[$pos], $job->getRunOnce()[$pos]?:null);
                     $cid = $this->getDbh()->lastInsertId("{$tp}job_step_id_seq");
                     $ids[] = $cid;
                     foreach($prevIds as $pid){
                        $this->exec_query("insert into {$tp}job_step_depends_on(job_step_id, depends_on_step_id) values(?,?)", $cid, $pid);
                     }
                     $this->log->trace(" $logPrefix <$jobName> Dependency rows have been inserted");
                     $subpos++;
                  }
              }else{
                 $this->log->trace(" $logPrefix <$jobName> Unknown data found");
                 throw new JobExecuteException("Unknown data in job $jobName");
              }
              $prevIds = $ids;
              $ids = [];
              $pos++;
            }

            $lastIds = $prevIds[count($prevIds)-1];
            $lastIds = is_array($lastIds) ? $lastIds : [ $lastIds ];
            foreach($dependants as $d){
                foreach($lastIds as $lid){
                    $this->exec_query("insert into {$tp}job_step_depends_on(job_step_id, depends_on_step_id) values(?,?)", $d, $lid);
                }
            }
            $this->releaseSavepoint();
            $this->log->debug(" $logPrefix <$jobName> Stored into repository");
            return $jobId;
      }catch(\Exception $e){
          $this->log->error(" $logPrefix <$jobName> Cannot store job into repository:" . $e->getMessage());
          $this->releaseSavepoint();
          throw new JobSubmitException("Cannot store $jobName into repository:" . $e->getMessage(), 0, $e);
      }
    }

    /**
     * Resumes failed or stopped job
     * @param int $jobId - id of the job
     * @return void
     * @throws JobSqlException
     * @throws JobTxnLevelUnderflow
     */
    function resumeJob(int $jobId){
        $this->setSavepoint();
        $tp = $this->tp;
        $rc = $this->exec_query("update {$tp}job_step set is_failed=false, try_count=case when try_count is null then null else 1 end where job_id=?", $jobId);
        if($rc)
            $this->exec_query("update {$tp}job set is_failed=false where id=?", $jobId);
        $this->releaseSavepoint();
    }

    /**
     * Deletes job regardless of its current state
     * @param int $jobId
     * @return void
     * @throws JobSqlException
     * @throws JobTxnLevelUnderflow
     * @throws JobDeleteException
     */
    function deleteJob($jobId){
        $this->setSavepoint();
        $tp = $this->tp;
        if(!$this->fetch_value("select 1 from {$tp}job where id=? for update", $jobId)){
          throw new JobDeleteException("Job with is=$jobId was not found");
        }
        $this->exec_query("delete from {$tp}job_step_depends_on where exists(select * from {$tp}job_step js where js.id=job_step_depends_on.job_step_id and js.job_id=?)", $jobId);
        $this->exec_query("delete from {$tp}job_step_depends_on where exists(select * from {$tp}job_step js where js.id=job_step_depends_on.depends_on_step_id and js.job_id=?)", $jobId);
        $this->exec_query("delete from {$tp}job_step where job_id=?", $jobId);
        $this->exec_query("delete from {$tp}job where id=?", $jobId);
        $this->releaseSavepoint();
    }

    /**
     * getter for log field
     * @return object
     */
    function getLog(){
        return $this->log;
    }
    /**
     * setter for log field
     * @param object $log
     * @return void
     */
    function setLog($log){
        $this->log = $log;
    }

    /**
     * @param string= $jobLike
     * @return array<array<string,mixed>>
     */
    function listJobs($jobLike = '%'){
        $tp = $this->tp;
        return $this->fetch_query("select * from {$tp}job where name like ?", $jobLike);
    }

    /**
     * @param string= $jobLike
     * @return array<array<string,mixed>>
     */
    function listEndedJobs($jobLike = '%'){
        $tp = $this->tp;
        return $this->fetch_query("select * from {$tp}job j where j.name like ? and j.is_done", $jobLike);
    }

    /**
     * @param string= $jobLike
     * @return array<array<string,mixed>>
     */
    function listNotEndedJobs($jobLike = '%'){
        $tp = $this->tp;
        return $this->fetch_query("select * from {$tp}job j where j.name like ? and not j.is_done", $jobLike);
    }

    /**
     * @param string= $jobLike
     * @return array<array<string,mixed>>
     */
    function listFailedJobs($jobLike = '%'){
        $tp = $this->tp;
        return $this->fetch_query("select * from {$tp}job j where j.name like ? and j.is_failed", $jobLike);
    }

    /**
     * @param int $jobId
     * @return int
     */
    function getJobLastStepId($jobId){
        $tp = $this->tp;
        return $this->fetch_value("select js.id from {$tp}job_step js where js.job_id=? order by js.pos desc limit 1", $jobId);
    }

    /**
     * @return void
     * @throws JobSqlException
     */
    function cleanUp(){
        $tp = $this->tp;
        $this->exec_query("delete from {$tp}job where not exists(select * from {$tp}job_step js where job.id=js.job_id) and job.is_done");
        $this->exec_query("delete from {$tp}job_step_depends_on where not exists(select * from {$tp}job_step js where js.id=job_step_depends_on.job_step_id)");
        $this->exec_query("delete from {$tp}job_step_depends_on where not exists(select * from {$tp}job_step js where js.id=job_step_depends_on.depends_on_step_id)");
    }

    /**
     * @return array<int>
     */
    function listDependantSteps(){
        $tp = $this->tp;
        $rv = [];
        foreach(
          $this->fetch_query("select jd.job_step_id
                              from {$tp}job_step_depends_on jd, {$tp}job_step js
                             where jd.depends_on_step_id=js.id and js.job_id=?", $this->getCurrentJobId())
          as $r){
           $rv[]=$r['job_step_id'];
        }
        return $rv;
    }

    /**
     * @param int= $limit
     * @return array<int>
     */
    function listNextSteps($limit=1){
        $tp = $this->tp;
        $rv = [];
        foreach($this->fetch_query("select js.id from {$tp}job_step js where js.job_id=? and js.pos>(select js1.pos from {$tp}job_step js1 where js1.id=?) order by js.pos limit ?",
                                   $this->getCurrentJobId(),
                                   $this->getCurrentStepId(),
                                   $limit
                                  ) as $r){
                $rv[] = $r['id'];
        }
        return $rv;
    }

    /** @param Job $job
     * @param array<string,mixed> $decoded_param
     * @param array<string,mixed> $decoded_val
     * @param array<string,mixed>|object $rvOrEx
     * @return void
     * @throws JobTxnLevelUnderflow
     * @internal
     */
    function handleCb($job, $decoded_param, $decoded_val, $rvOrEx=null){
             if($job->getCb()){
                   $this->log->trace("Run callback handler");
                   $this->setSavepoint();
                   try{
                       $cb = $job->getCb();
                       $cb($this, $job, $decoded_param, $decoded_val, $rvOrEx);
                       $this->log->trace("Callback handler is done");
                   }finally{
                       $this->releaseSavepoint();
                   }
             }
     }

    /**
     * @param string[]= $jobLike
     * @param callable():void= $callback
     * @param callable(array<string,mixed>):array<string,mixed>|false= $filterCallback
     * @return void
     * @throws JobTxnLevelUnderflow
     * @throws JobSqlException
     */
    function run($jobLike = [ '%' ], Callable $callback = null, Callable $filterCallback = null ){
        $logPrefix = 'JobExecutor#run[pid=' . getmypid() . ']';
        $this->log->debug(" $logPrefix started");
        $tp = $this->tp;
        $deadlockTryCount = 0;
        $toSleep = 0;
        while(1){
           $rs = $this->fetch_query("
                        select j1.*, j.name,
                          case when exists(select * from {$tp}job_step_depends_on jsd, {$tp}job_step js1 where jsd.job_step_id=js1.id and js1.job_id=j.id and jsd.depends_on_step_id=j1.id)
                                 or exists(select * from {$tp}job_step js where js.job_id=j.id and js.id<>j1.id)
                                 then 0 else 1 end as last_step
                         from {$tp}job_step j1,
                          {$tp}job j
                         where not exists(select * from {$tp}job_step_depends_on jsd, {$tp}job_step j2 where jsd.job_step_id=j1.id and jsd.depends_on_step_id=j2.id)
                           and not j1.is_failed
                           and not j.is_done
                           and not j.is_failed
                           and j1.run_after<=now()
                           and j.id=j1.job_id
                           and (" . join(' or ', array_map( function(string $v): string{ return 'j.name like ?';}, $jobLike)) . ')
                           limit 1000', ...$jobLike
                        );
           if(!$rs){
                if($callback) $callback();
                $this->log->debug(" $logPrefix No data, going to sleep and continue");
                $toSleep = $toSleep > 5 ? 5 : $toSleep+0.2;
                usleep($toSleep*1000000);
                continue;
           }
           $toSleep = 0;
           foreach($rs as $r){
              if($filterCallback){
                  $r = $filterCallback($r);
                  if(!$r) continue;
              }
              $this->log->debug(" $logPrefix database queried");
              $this->log->debug(" $logPrefix Got row to execute, trying to hold lock on it");
              $lockName = 'job-manager-' . $r['id'];

              $this->setSavepoint();
              $lock=$this->getLock($lockName);
              if(!$lock){
                $this->log->debug(" $logPrefix Record already locked by other process; will continue");
                $this->releaseSavepoint();
                continue;
              }
              $this->log->debug(" $logPrefix record locked");
              if($r['try_count']>0){
                  $this->log->debug(" $logPrefix try_count > 0, will try to decrease");
                  if(!$this->fetch_value("select try_count from {$tp}job_step js where js.id=? for update", $r['id'])){
                      $this->log->debug(" $logPrefix try_count > 0 and another process have just updated this record, will release lock and continue");
                      $this->releaseSavepoint();
                      $this->releaseLock($lockName);
                      continue;
                  }
                  $this->exec_query("update {$tp}job_step set try_count=try_count-1 where id=?", $r['id']);
              }elseif($r['try_count']===0 && $r['run_once']){
                  $this->log->debug(" $logPrefix try_count > 0 this job_step was set to runOnce and already has been run, set to failed, release locks and continue");
                  $this->exec_query("update {$tp}job_step set is_failed=true and try_count>0 where id=?", $r['id']);
                  $this->releaseSavepoint();
                  $this->releaseLock($lockName);
                  continue;
              }

              $job = $this->jobs[$r['name']];
              if(!$this->fetch_value("select 1 from {$tp}job_step js where not js.is_failed and js.id=? and run_after<now() for update", $r['id'])){
                  $this->log->debug(" $logPrefix Record was processed by another process; will continue");
                  $this->releaseSavepoint();
                  $this->releaseLock($lockName);
                  continue;
              }
              $this->log->debug(" $logPrefix Record locked; will do job {$job->getName()}");
              $fn = $job->getSteps()[$r['pos']];
              if(!$fn){
                 $this->log->error(" $logPrefix <{$job->getName()}> Cannot find step {$r['pos']} ");
                 $this->releaseSavepoint();
                 $this->releaseLock($lockName);
                 throw new JobRunException("Cannot find step {$r['pos']} for job {$r['name']}");
              }
              if(is_array($fn)){
                $fn = $fn[$r['subpos']];
              }
              if(!$fn){
                 $this->log->error(" $logPrefix <{$job->getName()}> Cannot find step {$r['pos']} ");
                 $this->releaseSavepoint();
                 $this->releaseLock($lockName);
                 throw new JobRunException("Cannot find step {$r['pos']} for job {$r['name']}");
              }

              list($param, $val) = $this->fetch_list("select parameters, val from {$tp}job j where j.id=?", $r['job_id']);
              try{
                 $decoded_param = (array)json_decode($param,true);
                 $decoded_val   = (array)json_decode($val,true);
                 if(!is_array($decoded_val)){
                    $decoded_val = [];
                 }
                 $this->log->trace("set savepoint");
                 $this->setSavepoint();
                 if($param && !is_array($decoded_param)){
                     throw new JobRunException("Passed param $param is not an array");
                 }
                 if($decoded_val && !is_array($decoded_val)){
                     throw new JobRunException("Context $val is not an array");
                 }


                 $time = time();
                 $this->setCurrentJobId($r['job_id']);
                 $this->setCurrentStepId($r['id']);
                  /** @var int $txnLevelBefore */
                 $txnLevelBefore = $this->getTxnLevel();
                 $this->handleCb($job, $decoded_param+['.state'=>'before'], $decoded_val);
                 $rv = $fn((array)$decoded_param, $decoded_val, $this, $r);
                 $this->handleCb($job, $decoded_param+['.state'=>'after'], $decoded_val,$rv);
                 $txnLevelAfter = $this->getTxnLevel();
                 if($txnLevelBefore!=$txnLevelAfter){
                    $this->getLog()->warn("It seems job(id=%s, name=%s step=%s)  left non-released or non-rollbacked savepoint: before level $txnLevelBefore!=$txnLevelAfter",
                                         [ $r['job_id'], $job->getName(), $r['id'] ]);
                 }
                 $this->setCurrentJobId(null);
                 $this->setCurrentStepId(null);
                 $deadlockTryCount = 0;
                 $this->log->debug(" $logPrefix <{$job->getName()}> Step #{$r['pos']} returned $val for parameters:{$param} context:{$val}");

                 $val2 = $this->fetch_value("select val from ${tp}job j where j.id=? for update", $r['job_id']);
                 $val2 = json_decode($val2,1);
                 if(!@$val2 || !is_array(@$val2)){
                      $val2=[];
                 }
                 if(!@$decoded_val || !is_array(@$decoded_val)){
                      $decoded_val=[];
                 }
                 $val = array_merge(@$val2,@$decoded_val);
                 $val2 = json_encode($val);

                 if($this->dbDriver == 'pgsql'){
                     $cnt = $this->exec_query("update {$tp}job set val=?, first_step_started_at=to_timestamp(?), last_step_finished_at=to_timestamp(?) where id=?", $val2, $time, time(), $r['job_id']);
                 }else{
                     $cnt = $this->exec_query("update {$tp}job set val=?, first_step_started_at=from_unixtime(?), last_step_finished_at=from_unixtime(?) where id=?", $val2, $time, time(), $r['job_id']);
                 }
                 if($cnt!=1){
                     throw new JobSqlException("Unexpected number of updated rows:$cnt");
                 }
                 if($r['last_step'] && !$rv || $rv=='DONE'){
                        $this->exec_query("update {$tp}job set is_done=true where id=?", $r['job_id']);
                        foreach( $this->fetch_query("select j.id
                                                          from {$tp}job j, {$tp}job_step js, {$tp}job_step_depends_on jsdo
                                                         where j.id=js.job_id and js.id=jsdo.job_step_id and jsdo.depends_on_step_id=?",
                                                   $r['id'])
                                 as $dj){
                            $jid = $dj['id'];
                            if($jid){
                                $djval = $this->fetch_value("select val from {$tp}job j where j.id=? for update", $jid);
                                $djval = json_decode($djval,1);
                                $djval = array_merge($val, $djval);
                                $this->exec_query("update {$tp}job set val=? where id=?", json_encode($djval), $jid);
                            }
                        }
                 }
                 if(!$rv || $rv=='STEP_DONE' || is_numeric($rv) && $rv<0){
                    $this->exec_query("delete from {$tp}job_step_depends_on where job_step_id=?", $r['id']);
                    $this->exec_query("delete from {$tp}job_step_depends_on where depends_on_step_id=?", $r['id']);
                    $this->exec_query("delete from {$tp}job_step where id=?", $r['id']);
                    if($rv=='DONE'){
                        $this->exec_query("delete from {$tp}job_step where job_id=?", $r['job_id']);
                    }
                    if(is_numeric($rv) && $rv<0){
                        $this->exec_query("update {$tp}job set is_failed=true where id=?", $r['job_id']);
                    }
                    $this->log->debug(" $logPrefix <{$job->getName()}> Step #{$r['pos']} done and deleted");
                 }elseif($rv instanceof \Exception){
                        $this->exec_query("update {$tp}job set is_failed=true, last_error=? where id=?", $rv->getMessage(), $r['job_id']);
                 }elseif(is_numeric($rv) || $rv=='CONTINUE'){
                     $wait = is_numeric($rv) ? $rv : 0;
                     if($this->dbDriver == 'pgsql'){
                        $this->exec_query("update {$tp}job_step js set run_after=coalesce(to_timestamp(?), now()) where js.id=?", time()+$wait, $r['id']);
                     }else{
                        $this->exec_query("update {$tp}job_step js set run_after=coalesce(from_unixtime(?), now()) where js.id=?", time()+$wait, $r['id']);
                     }
                 }elseif(is_array($rv)){
                    @list($jobName, $param, $ctx, $delay, $dependants) = $rv;
                    $dependants = $dependants ?: [];
                    if(!(is_scalar($jobName) && is_array($param) && is_array($ctx) && is_numeric($delay) && is_array($dependants))){
                         throw new \JobRunException("Invalid parameters have been returned from job for new job execution");
                    }
                    $this->exec_query("delete from {$tp}job_step_depends_on where job_step_id=?", $r['id']);
                    $this->exec_query("delete from {$tp}job_step_depends_on where depends_on_step_id=?", $r['id']);
                    $this->exec_query("delete from {$tp}job_step where id=?", $r['id']);
                    $jobId = $this->execute($jobName, $param, $ctx, $delay, [], $dependants);
                    $this->exec_query("insert into {$tp}job_step_depends_on(job_step_id, depends_on_step_id)
                                                   select * from(select j1.id, (select max(j2.id) from {$tp}job_step j2 where j2.job_id=?) as did from {$tp}job_step j1 where j1.job_id=? and j1.pos=?) as t where t.did is not null",
                                       $jobId, $r['job_id'], $r['pos']+1);
                 }elseif('DELETE'==$rv){
                    $this->exec_query("delete from {$tp}job_step where job_id=?", $r['job_id']);
                    $this->exec_query("update {$tp}job set is_done=true where id=?", $r['job_id']);
                 }
                 $this->log->trace("release savepoint");
                 $this->releaseSavepoint();
                 $this->log->debug(" $logPrefix <{$job->getName()}> Step #{$r['pos']} processed");
                 $this->releaseSavepoint();
                 $this->log->debug(" $logPrefix <{$job->getName()}> Step #{$r['pos']} completed");
              }catch(\Exception $e){
                 if(($e instanceof \PDOException && preg_match('/^40/', $e->errorInfo[0]) || preg_match('/[Dd]eadlock|Lock wait timeout exceeded/', $e->getMessage()))){
                    $deadlockTryCount++;
                    $this->log->info(" $logPrefix <{$job->getName()}> Step #{$r['pos']} serialization failure: {$e->getMessage()}");
                    $this->log->info(" $logPrefix <{$job->getName()}> Stack trace: {$e->getTraceAsString()}");
                    $this->rollbackToSavepoint();
                    $this->rollbackToSavepoint();
                    continue;
                 }
                 $this->log->debug(" $logPrefix <{$job->getName()}> Step #{$r['pos']} throws exception " . get_class($e) . ' with message ' . $e->getMessage());
                 $this->log->info(" $logPrefix <{$job->getName()}> Got exception:" . $e->getMessage());
                 $this->log->trace("rollback to savepoint");
                 $this->rollbackToSavepoint();
                 $this->exec_query("update {$tp}job_step set is_failed=true where id=?", $r['id']);
                 $this->exec_query("update {$tp}job set last_error=?, is_failed=true where id=?", $e->getMessage(), $r['job_id']);
                  /** @noinspection PhpUndefinedVariableInspection */
                  /** @noinspection PhpUndefinedVariableInspection */
                  $this->handleCb($job, $decoded_param+['.state'=>'exception'], $decoded_val, $e);
                 $this->releaseSavepoint();
                 continue;
              }finally{
                 $rv = $this->releaseLock($lockName);
                 $this->log->debug(" $logPrefix releaseLock returned $rv");
              }
            }
        }
    }

    /**
     * @param Job $job
     * @param array<string,mixed|array> $param
     * @param int= $times
     * @param array<Throwable> $throws
     * @param int= $throwCnt
     * @return void
     * @throws JobTxnLevelUnderflow
     */
    function stressTestJob(Job $job, array $param, int $times=2, array $throws=[], int $throwCnt=3){
        $steps = $job->getSteps();
        $ctx = [];
        foreach($steps as $st){
          if(is_callable($st)){
             while(true){
               $this->setSavepoint();
               for($j=0;$j<$times;$j++){
                   $savedCtx = $ctx;
                   while(true){
                       try{
                           $rv = $st($param,$ctx,$this);
                       } /** @noinspection PhpRedundantCatchClauseInspection */ catch(JobStressTestException $ex){
                           $ctx = $savedCtx;
                           continue;
                       }
                       break;
                   }
                   /** @noinspection PhpUndefinedVariableInspection */
                   if($rv==JobExecutor::DELETE_CURRENT_JOB) return;
                   if($j<$times-1) $ctx = $savedCtx;
               }
                 /** @noinspection PhpUndefinedVariableInspection */
                 if(!$rv){
                  $this->releaseSavepoint();
                  break;
               }
               if($rv instanceof \Exception) throw $rv;
               $this->releaseSavepoint();
               if(is_numeric($rv)){
                   $this->getLog()->info("testJob: timeout " . $rv);
                   sleep($rv);
               }
             }
          }elseif(is_array($st)){
             foreach($st as $s){
                 while(true){
                   $this->setSavepoint();
                   for($j=0;$j<$times;$j++){
                       $savedCtx = $ctx;
                       while(true){
                           try{
                               $rv = $s($param,$ctx,$this);
                           } /** @noinspection PhpRedundantCatchClauseInspection */ catch(JobStressTestException $ex){
                               $ctx = $savedCtx;
                               continue;
                           }
                           break;
                       }
                       /** @noinspection PhpUndefinedVariableInspection */
                       if($rv==JobExecutor::DELETE_CURRENT_JOB) return;
                       if($j<$times-1) $ctx = $savedCtx;
                   }
                     /** @noinspection PhpUndefinedVariableInspection */
                     if(!$rv){
                      $this->releaseSavepoint();
                      break;
                   }
                   if($rv instanceof \Exception) throw $rv;
                   $this->releaseSavepoint();
                   $this->getLog()->info("testJob: timeout " . $rv);
                   sleep($rv);
                 }
             }
          }else{
                 throw new \Exception("Unknown step type in job");
          }
        }
    }

    /**
     * @param Job $job
     * @param array<string,mixed|array> $param
     * @return void
     */
    function testJob($job, $param){
        $steps = $job->getSteps();
        $ctx = [];
        foreach($steps as $st){
          if(is_callable($st)){
             while(true){
               $this->setSavepoint();
               $rv = $st($param,$ctx,$this);
               if(!$rv){
                  $this->releaseSavepoint();
                  break;
               }
               if($rv instanceof \Exception) throw $rv;
               $this->releaseSavepoint();
               if($rv==JobExecutor::DELETE_CURRENT_JOB()) return;
               if(is_numeric($rv)){
                   $this->getLog()->info("testJob: timeout " . $rv);
                   sleep($rv);
               }
             }
          }elseif(is_array($st)){
             foreach($st as $s){
                if(is_callable($s)){
                     while(true){
                       $this->setSavepoint();
                       if(!$st($param,$ctx,$this)){
                          $this->releaseSavepoint();
                          break;
                       }
                       $this->releaseSavepoint();
                     }
                }else{
                     throw new \Exception("Unknown step type in job");
                }
             }
          }else{
                 throw new \Exception("Unknown step type in job");
          }
        }
    }

    function rvDelete(): string{
       return 'DELETE';
    }
    function rvContinue(): string{
       return 'CONTINUE';
    }
 }
 class sqlHelper{
    /** @var object */
    private $dbh = null;
    /** @var string */
    protected $dbDriver;

    /**
     * @return object
     */
    function getDbh(){
        return $this->dbh;
    }

    /**
     * @param object
     * @return void
     */
    function setDbh($pDbh){
        $this->dbh = $pDbh;
        $this->dbDriver = $this->getDbh()->getAttribute(\PDO::ATTR_DRIVER_NAME);
    }


    /*
      executes specified query with supplied param
      Last param can be a callback; if it is callback
      then it will be called with already executed, but not fetched statement
      If such callback is not specified then default callback will be used.
      The callback just returns affected rows
    */
    /** @suppress PhanPluginUnknownMethodReturnType
     *  @param string $query - sql query
    */
    public function execQuery(){
        return call_user_func_array([$this,'exec_query'],func_get_args());
    }

     /** @suppress PhanPluginUnknownMethodReturnType
      * @param string $qry - sql query
      * @param mixed ...$args - parameters for the query
      * @return int
      * @throws JobSqlException
      */
    public function exec_query(string $qry){
        $dbh = $this->getDbh();

        /**
         * @param object $sth
         * @return int
         */
        $cb = function($sth){
          $cnt =  $sth->rowCount();
          return $cnt;
        };

        $param = array_slice(func_get_args(),1);
        if($param and is_callable($param[count($param)-1])){
          $cb = $param[count($param)-1];
          if(count($param)>1){
            $param = array_slice($param, 0, count($param)-1);
          }else{
            $param=[];
          }
        }

        if(count($param)==1 && is_array($param[0]))
            $param=$param[0];

        $sth = $dbh->prepare($qry);
        if(!$sth){
            $err = $dbh->errorInfo(); $err = $err[2];
            throw new JobSqlException("Cannot prepare query:" . $err);
        }

        if(count($param)==1 && is_array($param[0]))
            $param=$param[0];

        if(!$sth->execute($param)){
            $err = $sth->errorInfo(); 
            throw new JobSqlException("Cannot exec query[SQLSTATE:{$err[0]}]:" . $err[2]);
        }

        return $cb($sth);
    }
    /*
      It's just a small wrapped about function above.
      The wrapped supplies the callbach which will fetch all rows and return array
    */
    /**
     * @param string $query
     * @param mixed ...$args
      * @return array<array<string,mixed>>
      */

    public function fetchQuery(){
        return call_user_func_array([$this,'fetch_query'],func_get_args());
    }
    /**
      @suppress PhanTypeMismatchReturn
      * @param string $query
      * @param mixed ...$args
      * @return array<array<string,mixed>>
      */
    public function fetch_query(){
      return call_user_func_array([$this,  'exec_query'],
                                    array_merge(func_get_args(),
                                    [
                                        /** @param object $sth
                                          * @return array<array<string,mixed>>
                                        */
                                        function($sth){
                                           $res = $sth->fetchAll();
                                           return $res;
                                        }
                                   ]
                                   )
                                 );
    }

    /**
      @suppress PhanTypeMismatchReturn
     * @param string $query
     * @param mixed ...$args
     * @return array<string,mixed>
      */
    public function fetchRow(){
        return call_user_func_array([$this,'fetch_row'],func_get_args());
    }
    /**
      @suppress PhanTypeMismatchReturn
      * @param string $query
      * @param mixed ...$args
      * @return array<string,mixed>
      */
    public function fetch_row(){
      return call_user_func_array( [$this, 'exec_query'],
                                    array_merge(func_get_args(),
                                    [
                                        /** @param object $sth
                                          * @return array<array<string,mixed>>
                                          */
                                        function($sth){
                                           $res = $sth->fetch();

                                           if($res)
                                              return $res;
                                           else
                                             return [];
                                        }
                                    ]
                                   )
                                 );
    }

    /**
     * @param string $query
     * @param mixed ...$args
      * @return array<mixed>
      */
    public function fetchList(){
        return call_user_func_array([$this,'fetch_list'],func_get_args());
    }

    /**
      @suppress PhanTypeMismatchReturn
     * @param string $query
     * @param mixed ...$args
      * @return array<mixed>
      */
    public function fetch_list(){
      return call_user_func_array( [ $this, 'exec_query'],
                                    array_merge(func_get_args(),
                                    [
                                        /** @param object $sth
                                          * @return array<array<string,mixed>>
                                          */
                                        function($sth){
                                           $res = $sth->fetch(\PDO::FETCH_NUM);

                                           if($res)
                                              return $res;
                                           else
                                             return [];
                                        }
                                    ]
                                   )
                                 );
    }

    /**
     * @param string $query
     * @param mixed ...$args
      * @return mixed
      */
    public function fetchValue(){
      return @call_user_func_array([$this, 'fetch_list'], func_get_args())[0];
    }

    /**
     * @param string $query
     * @param mixed ...$args
      * @return mixed
      */
    function fetch_value(){
      return @call_user_func_array([$this, 'fetch_list'], func_get_args())[0];
    }

    /**
      * @param string $sqname
      * @return int
      */
    public function lastInsertId($sqname=null){
        if($this->dbDriver == 'pgsql')
          return $this->getDbh()->lastInsertId($sqname);
        else
          return $this->getDbh()->lastInsertId();
    }

    /**
      * @param string $lockName
      * @return int
      */
    public function getLock($lockName){
        if($this->dbDriver == 'pgsql'){
           return $this->fetch_value('select pg_try_advisory_xact_lock(?)', crc32($lockName));
        }else{
           return $this->fetch_value('select get_lock(?,0)', $lockName);
        }
    }

    /**
      * @param string $lockName
      * @return int
      */
    public function releaseLock($lockName){
        if($this->dbDriver == 'pgsql'){
           return 1;
        }else{
           return $this->fetch_value('select release_lock(?)', $lockName);
        }
    }

    /* transaction management
      As we can call JobExecutor::execute inside step functions we cannot commit there,
      but only release savepoint; so in general case inside function we don't know
      what we must do - start transaction or set savepoint? release savepoint or commit?
      rollback transaction or just rollback to savepoint?
    */
    /** @var int */
    private $txnLevel=0;
    /**
     * @return int
     */
    public function getTxnLevel(){
       return $this->txnLevel;
    }
    /**
     * @return void
     */
    public function setSavepoint(){
        if($this->txnLevel==0){
            if($this->dbDriver == 'pgsql'){
                $this->exec_query("begin transaction");
            }else
                $this->exec_query("start transaction");
        }else{
            $this->exec_query("savepoint job_exec_{$this->txnLevel}");
        }
        $this->txnLevel++;
    }

    /**
     * @return void
     */
    public function releaseSavepoint(){
        $this->txnLevel--;
        if($this->txnLevel<0){
           $this->txnLevel=0;
           throw new JobTxnLevelUnderflow('txnLevel underflow');
        }
        if($this->txnLevel==0){
            $this->exec_query("commit");
        }else{
            $this->exec_query("release savepoint job_exec_{$this->txnLevel}");
        }
    }

    /**
     * @return void
     */
    public function rollback(){
        $this->exec_query("rollback");
        $this->txnLevel=0;
    }

    /**
     * @return void
     */
    public function commit(){
        $this->exec_query("commit");
        $this->txnLevel=0;
    }
    /**
     * @return void
     */
    public function rollbackToSavepoint(){
        $this->txnLevel--;
        if($this->txnLevel<0){
           $this->txnLevel=0;
           throw new JobTxnLevelUnderflow('txnLevel underflow');
        }

        if($this->txnLevel==0){
            $this->exec_query("rollback");
        }else{
            $this->exec_query("rollback to savepoint job_exec_{$this->txnLevel}");
        }
    }
}

/**
 @param array<mixed|array> $stacktrace
 @return string
 */
function callStack(array $stacktrace) {
    $rv = "";
    foreach($stacktrace as $node) {
        $rv .= basename($node['file']) .":" .$node['function'] ."(" .$node['line'].")\n";
    }
    return $rv;
}
