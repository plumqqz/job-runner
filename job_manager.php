<?php
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
  job_step_id bigint not null references job_step(id),
  depends_on_step_id bigint not null references job_step(id),
  primary key(job_step_id, depends_on_step_id)
)


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
  run_after timestamp without time zone,
  CONSTRAINT job_step_pkey PRIMARY KEY (id),
  CONSTRAINT job_step_job_id_fkey FOREIGN KEY (job_id)
      REFERENCES public.job (id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
);
CREATE TABLE public.job_step_depends_on
(
  job_step_id bigint NOT NULL,
  depends_on_step_id bigint NOT NULL,
  CONSTRAINT job_step_depends_on_pkey PRIMARY KEY (job_step_id, depends_on_step_id),
  CONSTRAINT job_step_depends_on_depends_on_step_id_fkey FOREIGN KEY (depends_on_step_id)
      REFERENCES public.job_step (id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT job_step_depends_on_job_step_id_fkey FOREIGN KEY (job_step_id)
      REFERENCES public.job_step (id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
);
*/
    class JobSubmitException extends Exception{};
    class JobExecuteException extends Exception{};
    class JobRunException extends Exception{};
    class JobLogger{
       const PANIC = 0;
       const ERROR = 1;
       const WARN  = 2;
       const WARNING = 2;
       const INFO = 3;
       const DEBUG = 4;
       const TRACE = 5;

       private $lastLogFileName = null;
       private $logFile = null;
       private $level;

       function __construct($ll){
            $this->level = $ll;
       }

       function openFile(){
            $logFileName = 'job-runner-' . date('Y-m-d') . '.log';
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
       function writeToLog($str){
            $str = preg_replace("/\r?\n|\n\r?/",'\\n',$str);
            if($this->logFile)
                fwrite($this->logFile, "$str\n");
            else{
                error_log("Log destination file is not opened; error message is $msg");
            }
       }
       function printLine($level, $msg, $opt = null){
            $this->openFile();
            $msg = vsprintf($msg, $opt);
            $outString = 'job-runner [' . date('Y-m-d H-i-s') . "] $level: $msg";
            $this->writeToLog($outString);
       }
       function panic($msg, $opt = null){
            if($this->level <= JobLogger::PANIC)
                return;
            $this->printLine('PANIC', $msg, $opt);
       }
       function error($msg, $opt = null){
            if($this->level <= JobLogger::ERROR)
                return;
            $this->printLine('ERROR', $msg, $opt);
       }
       function warn($msg, $opt = null){
            if($this->level <= JobLogger::WARN)
                return;
            $this->printLine('WARN', $msg, $opt);
       }
       function info($msg, $opt = null){
            if($this->level <= JobLogger::INFO)
                return;
            $this->printLine('INFO', $msg, $opt);
       }
       function debug($msg, $opt = null){
            if($this->level <= JobLogger::DEBUG)
                return;
            $this->printLine('DEBUG', $msg, $opt);
       }
       function trace($msg, $opt = null){
            if($this->level <= JobLogger::TRACE)
                return;
            $this->printLine('TRACE', $msg, $opt);
       }
        
    }

    class Job{
        private $steps = [];
        private $runOnce = [];
        private $name;
        private $param;
        private $log;
        private $cb;

        function __construct($name){
            $this->name = $name;
            $this->log = new JobLogger(getenv("JOB_MANAGER_LOGLVL") ?: JobLogger::INFO);
            $this->log->debug(" <{$this->name}> Create new job with name $name");
            return $this;
        }
        function getName(){
            return $this->name;
        }

        function getSteps(){
            return $this->steps;
        }

        function getRunOnce(){
            return $this->runOnce;
        }

        function submitOnce($param){
            return $this->submit($param, true);
        }

        function setCallback($fn){
            $this->setCb($fn);
        }
        function setCb($fn){
            if(!is_callable($fn)){
                throw new JobSubmitException("Passed param is not function");
            }
            $this->cb = $fn;
        }
        function getCb(){
            return $this->cb;
        }
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
        
        function end(){
            return;
        }
    }

class JobExecutor extends sqlHelper{
    private $jobs=[];
    private $tp="";
    private $log;
    private $callCount=0;

    function __construct($dbh=null, $tp = "", $logger=null){
         $this->log = $logger ?: new JobLogger(getenv("JOB_MANAGER_LOGLVL") ?: JobLogger::TRACE);
         $this->tp = $tp;
         if($dbh)
            $this->setDbh($dbh);

         $job = new Job('.execute');
         $job->submit(function($param, &$ctx, $je){
             try{
                 $je->execute(@$param['name'], @$param['param'], @$param['ctx'], @$param['delay']);
             }catch(Exception $e){
                 $this->log->error("Internal executor: Cannot submit job {$param['name']} : {$e->getMessage()}");
             }
         });
         $this->add($job);
    }

    function add(Job $job){
        $this->log->debug("JobExecutor: add new job {$job->getName()}");
        $arr = $job->getSteps();
        if(is_array($arr[count($arr)-1]))
            $job->submit(function(){});
        $this->jobs[$job->getName()] = $job;
    }

    function execute($jobName, $param, $ctx = null, $delay = 0, $depends_on = []){
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
            $rc = 0;
            if($this->dbDriver == 'pgsql'){
                $rc = $this->exec_query("insert into {$tp}job(parameters, val, name, hash ) values(?,?,?,?) on conflict(name,hash) do nothing", json_encode($param), $ctx ?: '{}', $jobName, $hash);
            }else{
                $rc = $this->exec_query("insert into {$tp}job(parameters, val, name, hash ) values(?,?,?,?) on duplicate key update hash=hash", json_encode($param), $ctx ?: '{}', $jobName, $hash);
            }
            if(!$rc){
                 $this->releaseSavepoint();
                 $this->log->debug(" $logPrefix <$jobName> Job $jobName with parameters hash equals to $hash already has been executed. Returning");
                 return;
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
          $this->releaseSavepoint();
          $this->log->debug(" $logPrefix <$jobName> Stored into repository");
          return $jobId;
      }catch(Exception $e){
          $this->exec_query("rollback");
          $this->log->error(" $logPrefix <$jobName> Cannot store job into repository");
          throw new Exception("Cannot store $jobName into repository",0, $e);
      }
    }

    function resumeJob($jobId){
        $this->setSavepoint();
        $tp = $this->tp;
        $rc = $this->exec_query("update {$tp}job_step set is_failed=false, try_count=case when try_count is null then null else 1 end where job_id=?", $jobId);
        if($rc)
            $this->exec_query("update {$tp}job set is_failed=false where id=?", $jobId);
        $this->releaseSavepoint();
    }

    function listJobs($jobLike = '%'){
        $tp = $this->tp;
        return $this->fetch_query("select * from {$tp}job where name like ?", $jobLike);
    }

    function listEndedJobs($jobLike = '%'){
        $tp = $this->tp;
        return $this->fetch_query("select * from {$tp}job j where j.name like ? and j.is_done", $jobLike);
    }

    function listNotEndedJobs($jobLike = '%'){
        $tp = $this->tp;
        return $this->fetch_query("select * from {$tp}job j where j.name like ? and not j.is_done", $jobLike);
    }

    function listFailedJobs($jobLike = '%'){
        $tp = $this->tp;
        return $this->fetch_query("select * from {$tp}job j where j.name like ? and j.is_failed", $jobLike);
    }

    function getJobLastStepId($jobId){
        return $this->fetch_value("select js.id from {$tp}job_step js where js.job_id=? order by js.pos desc limit 1", $jobId);
    }

    function cleanUp(){
        $this->exec_query("delete from {$tp}job where not exists(select * from ${tp}job_step js where job.id=js.job_id)");
    }

    function run($jobLike = [ '%' ]){
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
                           and (" . join(' or ', array_map( function($v){ return 'j.name like ?';}, $jobLike)) . ')
                           limit 100', ...$jobLike
                        );
           if(!$rs){
            $this->log->debug(" $logPrefix No data, going to sleep and continue");
            $toSleep = $toSleep > 5 ? 5 : $toSleep+0.2;
            usleep($toSleep*1000000);
            continue;
           }
           $toSleep = 0;
           foreach($rs as $r){
              $this->log->debug(" $logPrefix database queried");
              $this->log->debug(" $logPrefix Got row to execute, trying to hold lock on it");

              $lock=$this->getLock('job-manager-' . $r['id']);
              if(!$lock){
                $this->log->debug(" $logPrefix Record already locked by other process; will continue");
                continue;
              }
              $this->log->debug(" $logPrefix record locked");
              if($r['try_count']>0){
                  $this->log->debug(" $logPrefix try_count > 0, will try to decrease");
                  $this->setSavepoint();
                  if(!$this->fetch_value("select try_count from {$tp}job_step js where js.id=? for update", $r['id'])){
                      $this->log->debug(" $logPrefix try_count > 0 and another process have just updated this record, will release lock and continue");
                      $this->releaseSavepoint();
                      $this->releaseLock('job-manager-' . $r['id']);
                      continue;
                  }
                  $this->exec_query("update {$tp}job_step set try_count=try_count-1 where id=?", $r['id']);
                  $this->releaseSavepoint();
              }elseif($r['try_count']===0 && $r['run_once']){
                  $this->log->debug(" $logPrefix try_count > 0 this job_step was set to runOnce and already has been run, set to failed, release locks and continue");
                  $this->exec_query("update {$tp}job_step set is_failed=true and try_count>0 where id=?", $r['id']);
                  $this->releaseLock('job-manager-' . $r['id']);
                  continue;
              }
              $this->setSavepoint();

              $job = $this->jobs[$r['name']];
              if(!$this->fetch_value("select 1 from {$tp}job_step js where not js.is_failed and js.id=? for update", $r['id'])){
                  $this->log->debug(" $logPrefix Record was processed by another process; will continue");
                  $this->releaseSavepoint();
                  $this->releaseLock('job-manager-' . $r['id']);
                  continue;
              }
              $this->log->debug(" $logPrefix Record locked; will do job {$job->getName()}");
              $fn = $job->getSteps()[$r['pos']];
              if(!$fn){
                 $this->log->error(" $logPrefix <{$job->getName()}> Cannot find step {$r['pos']} ");
                 $this->releaseSavepoint();
                 $this->releaseLock('job-manager-' . $r['id']);
                 throw new JobRunException("Cannot find step {$r['pos']} for job {$r['name']}");
              }
              if(is_array($fn)){
                $fn = $fn[$r['subpos']];
              }
              if(!$fn){
                 $this->log->error(" $logPrefix <{$job->getName()}> Cannot find step {$r['pos']} ");
                 $this->releaseSavepoint();
                 $this->releaseLock('job-manager-' . $r['id']);
                 throw new JobRunException("Cannot find step {$r['pos']} for job {$r['name']}");
              }

              list($param, $val) = $this->fetch_list("select parameters, val from {$tp}job j where j.id=?", $r['job_id']);
              try{
                 $decoded_param = json_decode($param,1);
                 $decoded_val   = json_decode($val,1);
                 $this->log->trace("set savepoint");
                 $this->setSavepoint();
                 if($param && !is_array($decoded_param)){
                     throw new JobRunException("Passed param $param is not an array");
                 }
                 if($decoded_val && !is_array($decoded_val)){
                     throw new JobRunException("Context $val is not an array");
                 }

                 if($job->getCb()){
                       $this->log->trace("Run callback handler");
                       $this->setSavepoint();
                       $cb = $job->getCb();
                       $cb($decoded_param, $decoded_val, $this);
                       $this->log->trace("Callback handler is done");
                       $this->releaseSavepoint();
                 }


                 $time = time();
                 $rv = $fn($decoded_param, $decoded_val, $this, $r);
                 $deadlockTryCount = 0;
                 $this->log->debug(" $logPrefix <{$job->getName()}> Step #{$r['pos']} returned $val for parameters:{$param} context:{$val}");

                 $val2 = $this->fetch_value("select val from ${tp}job j where j.id=? for update", $r['job_id']);
                 $val2 = json_decode($val2,1);
                 $val = array_merge($val2,$decoded_val);
                 $val = json_encode($decoded_val);

                 if($this->dbDriver == 'pgsql'){
                     $this->exec_query("update {$tp}job set val=?, first_step_started_at=to_timestamp(?), last_step_finished_at=to_timestamp(?) where id=?", $val, $time, time(), $r['job_id']);
                 }else{
                     $this->exec_query("update {$tp}job set val=?, first_step_started_at=from_unixtime(?), last_step_finished_at=from_unixtime(?) where id=?", $val, $time, time(), $r['job_id']);
                 }
                 if($r['last_step'] && !$rv){
                        $this->exec_query("update {$tp}job set is_done=true where id=?", $r['job_id']);
                        @list($djval, $jid) = $this->fetch_list("select j.val, j.id
                                                          from {$tp}job j, {$tp}job_step js, {$tp}job_step_depends_on jsdo 
                                                         where j.id=js.job_id and js.id=jsdo.job_step_id and jsdo.depends_on_step_id=?
                                                         for update
                                                         limit 1
                                                         ",
                                           $r['id']);
                        $this->log->debug("Try to update caller context: val = $val");
                        if($jid){
                            $djval = json_decode($djval,1);
                            $djval = array_merge($decoded_val, $djval);
                            $this->exec_query("update {$tp}job set val=? where id=?", json_encode($djval), $jid);
                        }

                 }
                 if(!$rv || is_numeric($rv) && $rv<0){
                    $this->exec_query("delete from {$tp}job_step_depends_on where job_step_id=?", $r['id']);
                    $this->exec_query("delete from {$tp}job_step_depends_on where depends_on_step_id=?", $r['id']);
                    $this->exec_query("delete from {$tp}job_step where id=?", $r['id']);
                    if(is_numeric($rv) && $rv<0){
                        $this->exec_query("update {$tp}job set is_failed=true where id=?", $r['job_id']);
                    }
                    $this->log->debug(" $logPrefix <{$job->getName()}> Step #{$r['pos']} done and deleted");
                 }elseif(is_numeric($rv)){
                     $wait = $rv;
                     if($this->dbDriver == 'pgsql'){
                        $this->exec_query("update {$tp}job_step js set run_after=coalesce(to_timestamp(?), now()) where js.id=?", time()+$wait, $r['id']);
                     }else{
                        $this->exec_query("update {$tp}job_step js set run_after=coalesce(from_unixtime(?), now()) where js.id=?", time()+$wait, $r['id']);
                     }
                 }elseif(is_array($rv)){
                    list($jobName, $param, $ctx, $delay) = $rv;
                    if(!(is_scalar($jobName) && is_array($param) && is_array($ctx) && is_numeric($delay))){
                         throw new \Exception("Invalid parameters have been returned from job for new job execution");
                    }
                    $this->exec_query("delete from {$tp}job_step_depends_on where job_step_id=?", $r['id']);
                    $this->exec_query("delete from {$tp}job_step_depends_on where depends_on_step_id=?", $r['id']);
                    $this->exec_query("delete from {$tp}job_step where id=?", $r['id']);
                    $jobId = $this->execute($jobName, $param, $ctx, $delay);
                    $this->exec_query("insert into {$tp}job_step_depends_on(job_step_id, depends_on_step_id)
                                                   select * from(select j1.id, (select max(j2.id) from {$tp}job_step j2 where j2.job_id=?) as did from {$tp}job_step j1 where j1.job_id=? and j1.pos=?) as t where t.did is not null",
                                       $jobId, $r['job_id'], $r['pos']+1);
                 }
                 $this->log->trace("release savepoint");
                 $this->releaseSavepoint();
                 $this->log->debug(" $logPrefix <{$job->getName()}> Step #{$r['pos']} processed");
              }catch(\Exception $e){
                 if($e instanceof \PDOException && preg_match('/^40/', $exc->errorInfo[0]) && $deadlockTryCount>5){
                    $deadlockTryCount++;
                    $this->log->debug(" $logPrefix <{$job->getName()}> Step #{$r['pos']} serialization failure: {$e->getMessage()}");
                    $this->rollbackToSavepoint();
                    $this->releaseSavepint();
                    $this->releaseLock('job-manager-' . $r['id']);
                    continue;
                 }
                 $this->log->debug(" $logPrefix <{$job->getName()}> Step #{$r['pos']} throws exception " . get_class($e) . ' with message ' . $e->getMessage());
                 $this->log->trace("rollback to savepoint");
                 $this->rollbackToSavepoint();
                 $this->exec_query("update {$tp}job_step set is_failed=true where id=?", $r['id']);
                 $this->exec_query("update {$tp}job set last_error=?, is_failed=true where id=?", $e->getMessage(), $r['job_id']);
              }
              $this->releaseSavepoint();
              $this->log->debug(" $logPrefix <{$job->getName()}> Step #{$r['pos']} completed");
              $this->releaseLock('job-manager-' . $r['id']);
            }
        }
    }
 }
 class sqlHelper{
    private $dbh = null;
    protected $dbDriver;

    function getDbh(){
        return $this->dbh;
    }

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
    function exec_query($qry){
        $dbh = $this->getDbh();

        $cb = function($sth){
          return $sth->rowCount();
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

        $sth = $dbh->prepare($qry);
        if(!$sth){
            $err = $dbh->errorInfo(); $err = $err[2];
            throw new \Exception("Cannot prepare query:" . $err);
        }

        if(!$sth->execute($param)){
            $err = $sth->errorInfo(); $err = $err[2];
            throw new \Exception("Cannot exec query:" . $err);
        }

        return $cb($sth);
    }
    /*
      It's just a small wrapped about function above.
      The wrapped supplies the callbach which will fetch all rows and return array
    */
    function fetch_query($query){
      return call_user_method_array('exec_query', $this,
                                    array_merge(func_get_args(),
                                    [
                                        function($sth){
                                           $res = $sth->fetchAll();
                                           return $res;
                                        }
                                   ]
                                   )
                                 );
    }

    function fetch_row(){
      return call_user_method_array( 'exec_query', $this,
                                    array_merge(func_get_args(),
                                    [
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

    function fetch_list(){
      return call_user_method_array(  'exec_query' , $this,
                                    array_merge(func_get_args(),
                                    [
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

    function fetch_value(){
      return @call_user_method_array('fetch_list', $this, func_get_args())[0];
    }

    function lastInsertId($sqname){
        if($this->dbDriver == 'pgsql')
          return $this->getDbh()->lastInsertId($sqname);
        else
          return $this->getDbh()->lastInsertId();
    }

    function getLock($lockName){
        if($this->dbDriver == 'pgsql'){
           $this->exec_query('select pg_advisory_xact_lock(?)', crc32($lockName));
           return 1;
        }else{
           return $this->fetch_value('select get_lock(?,0)', $lockName);
        }
    }

    function releaseLock($lockName){
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
    private $txnLevel=0;
    function setSavepoint(){

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

    function releaseSavepoint(){
        $this->txnLevel--;
        if($this->txnLevel==0){
            $this->exec_query("commit");
        }else{
            $this->exec_query("release savepoint job_exec_{$this->txnLevel}");
        }
    }

    function rollbackToSavepoint(){
        $this->txnLevel--;
        if($this->txnLevel==0){
            $this->exec_query("rollback");
        }else{
            $this->exec_query("rollback to savepoint job_exec_{$this->txnLevel}");
        }
    }


}
