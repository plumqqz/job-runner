<?php
/*
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
   first_step_started_at datetime
)


create table job_step(
   id bigint auto_increment primary key,
   job_id bigint not null references job(id),
   pos int not null,
   subpos int,
   is_failed boolean default false,
   run_after datetime
)

create table step_depends_on(
  job_step_id bigint not null references job_step(id),
  depends_on_step_id bigint not null references job_step(id),
  primary key(job_step_id, depends_on_step_id)
)
*/
    class JobSubmitException extends Exception{};
    class JobExecuteException extends Exception{};

    class Job{
        private $steps = [];
        private $name;
        private $param;

        function __construct($name){
            $this->name = $name;
            return $this;
        }
        function getName(){
            return $this->name;
        }

        function getSteps(){
            return $this->steps;
        }

        function submit($param){
            if(is_callable($param)){
                $this->steps[] = $param;
            }elseif(is_array($param)){
                foreach($param as $p){
                    if(!is_callable($p))
                        throw new JobSubmitException("Passed array has non-function element");
                }
                $this->steps[] = $param;
            }else{
                throw new JobSubmitException("Passed param neither function nor array of functions");
            }
            return $this;
        }
        
        function end(){
            return;
        }
    }

class JobExecutor{
    private $jobs=[];
    private $tp="";
    private $dbh = null;
    function getDbh(){
        return $this->dbh;
    }

    function setDbh($pDbh){
        $this->dbh = $pDbh;
    }
    function add(Job $job){
        $this->jobs[$job->getName()] = $job;
    }

    function execute($jobName, $param, $ctx = null){
      if(!is_string($jobName)){
         throw new JobExecuteException("Passed jobName is not a string");
      }
      if(!is_array($param)){
         throw new JobExecuteException("Passed param is not a string");
      }

      if($ctx && !is_array($ctx)){
         throw new JobExecuteException("Passed ctx is not a string");
      }

      ksort($param);
      $tp = $this->tp;
      $hash = md5(serialize($param));

      if($this->fetch_value("select 1 from {$tp}job j where j.name=? and j.hash=?", $jobName, $hash)){
         return;
      }
      
      $job = $this->jobs[$jobName];

      if(!$job){
         throw new Exception("Cannot find job $jobName");
      }

      try{
          $this->exec_query("start transaction");
            $this->exec_query("insert into {$tp}job(parameters, val, name, hash ) values(?,?,?,?)", json_encode($param), $ctx ?: '{}', $jobName, $hash);
            $jobId = $this->fetch_value('select last_insert_id()');
            $ids=[]; $prevIds=[];
            $pos=0;
            foreach($job->getSteps() as $s){
              if(is_callable($s)){
                  $this->exec_query("insert into {$tp}job_step(job_id, pos, subpos, run_after) values(?,?,null,'2001-01-01')", $jobId, $pos);
                  $cid = $this->fetch_value('select last_insert_id()');
                  $ids = [ $cid ];
                  foreach($prevIds as $pid){
                  	$this->exec_query("insert into {$tp}job_step_depends_on(job_step_id, depends_on_step_id) values(?,?)", $cid, $pid);
                  }
              }elseif(is_array($s)){
                  $subpos=0;
                  foreach($s as $s1){
                     $this->exec_query("insert into {$tp}job_step(job_id, pos, subpos, run_after) values(?,?,?,'2001-01-01')", $jobId, $pos, $subpos);
                     $cid = $this->fetch_value('select last_insert_id()');
                     $ids[] = $cid;
                     foreach($prevIds as $pid){
                     	$this->exec_query("insert into {$tp}job_step_depends_on(job_step_id, depends_on_step_id) values(?,?)", $cid, $pid);
                     }
                     $subpos++;
                  }
              }else{
                 throw new Exception("Unknown data in job $jobName");
              }
              $prevIds = $ids;
              $ids = [];
              $pos++;
            }
          $this->exec_query("commit");
      }catch(Exception $e){
          $this->exec_query("rollback");
          throw new Exception("Cannot stope $jobName into repository",0, $e);
      }
    }

    function run(){
        $tp = $this->tp;
        while(1){
           $r = $this->fetch_row(<<<"EOT"
                        select j1.*, j.parameters, j.val, j.name
                          from {$tp}job_step j1, {$tp}job j
                         where not exists(select * from {$tp}job_step_depends_on jsd, {$tp}job_step j2 where jsd.job_step_id=j1.id and jsd.depends_on_step_id=j2.id)
                           and not j1.is_failed 
                           and j1.run_after<now()
                           and j.id=j1.job_id
                           limit 1
EOT
                                 );
          if(!$r){
            sleep(3);
            continue;
          }
          $lock=$this->fetch_value('select get_lock(?,0)', 'job-manager-' . $r['id']);
          if(!$lock){
            continue;
          }
          $job = $this->jobs[$r['name']];
          $fn = $job->getSteps()[$r['pos']];
          if(!$fn){
             throw new Exception("Cannot find step {$r['pos']} for job {$r['name']}");
          }
          if(is_array($fn)){
            $fn = $fn[$r['subpos']];
          }
          if(!$fn){
             throw new Exception("Cannot find step {$r['pos']} for job {$r['name']}");
          }

          $param = json_decode($r['parameters'],1);
          $val   = json_decode($r['val'],1);

          $this->exec_query('start transaction');
          try{
             $this->exec_query('savepoint job_svp');
             $time = time();
             $rv = $fn($param, $val);
             $old_val = $this->fetch_value("select j.val from {$tp}job j where id=? for update", $r['job_id']);
             $old_val = json_decode($old_val,1);
             $val = json_encode($val+$old_val);
             $this->exec_query("update {$tp}job set val=?, first_step_started_at=from_unixtime(?), last_step_finished_at=now() where id=?", $val, $time, $r['job_id']);
             if(!$rv){
                $this->exec_query("delete from {$tp}job_step_depends_on where job_step_id=?", $r['id']);
                $this->exec_query("delete from {$tp}job_step where id=?", $r['id']);
             }
             $this->exec_query('release savepoint job_svp');
          }catch(Exception $e){
             $this->exec_query('rollback to savepoint job_svp');
             $this->exec_query("update {$tp}job_step set is_failed=true where id=?", $r['id']);
             $this->exec_query("update {$tp}job set last_error=?, is_failed=true where id=?", $e->getMessage(), $r['job_id']);
 
          }
          $this->exec_query('commit');
		  $this->fetch_value('select release_lock(?)', 'job-manager-' . $r['id']);
        }
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

}