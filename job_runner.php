<?php

/*

CREATE TABLE public.job_runner_job
(
  id bigint NOT NULL DEFAULT nextval('job_runner_job_id_seq'::regclass),
  name text NOT NULL,
  pos integer,
  subpos integer,
  run_after timestamp with time zone,
  depends_on numeric[],
  is_broken boolean DEFAULT false,
  last_run timestamp with time zone,
  last_error text,
  CONSTRAINT job_runner_job_pkey PRIMARY KEY (id)
);

CREATE INDEX job_runner_job_name_pos_idx
  ON public.job_runner_job
  USING btree
  (name COLLATE pg_catalog."C", pos) where not is_done;

-- Index: public.job_runner_job_name_run_after_idx

-- DROP INDEX public.job_runner_job_name_run_after_idx;

CREATE INDEX job_runner_job_name_run_after_idx
  ON public.job_runner_job
  USING btree
  (name COLLATE pg_catalog."C", run_after) where not is_done;


CREATE TABLE public.job_runner_job_param
(
  job_runner_job_id bigint NOT NULL,
  param jsonb,
  CONSTRAINT job_runner_job_param_pkey PRIMARY KEY (job_runner_job_id),
  CONSTRAINT job_runner_job_param_job_runner_job_id_fkey FOREIGN KEY (job_runner_job_id)
      REFERENCES public.job_runner_job (id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE cascade
);


*/

namespace JobRunner{
    class JobSubmitException extends \Exception {};

    class Job{
        private $jobs = [];
        private $name;
        private $param;

        function __construct($name){
            $this->name = $name;
            return $this;
        }
        function getName(){
            return $this->name;
        }

        function getJobs(){
            return $this->jobs;
        }

        function submit($param){
            if(is_callable($param)){
                $this->jobs[] = $param;
            }elseif(is_array($param)){
                foreach($param as $p){
                    if(!is_callable($p))
                        throw new JobSubmitException("Passed array has non-function element");
                }
                $this->jobs[] = $param;
            }else{
                throw new JobSubmitException("Passed param neither function nor array of functions");
            }
            return $this;
        }
        
        function end(){
            return;
        }
    }

    class JobReturnValue{
      public $jobExecutor;
      public $resumitInterval = 0;
      public $delayInterval;
      public $row;
      public function setResubmitInterval($int){
        $this->resumitInterval = $int;
      }
      public function getResubmitInterval(){
        return $this->resumitInterval;
      }
    }

    class JobExecutor{
        private $jobs = [];
        function add(Job $job){
            $this->jobs[$job->getName()] = $job;
        }
        function submit($jobName, $param = null){
            if($param && !is_array($param)){
                throw new JobSubmitException("Job parametes passed to submit must be an array of parameters for each step");
            }
            exec_query('begin transaction');
                $job = $this->jobs[$jobName];
                $ids=[]; $prevIds=[];
                foreach($job->getJobs() as $k=>$v){
                    if(!is_array($v)){
                        $id = fetch_value("insert into job_runner_job(name, run_after, pos, depends_on)
                                                               values(?,    ?, ?,  ?::bigint[])
                                                               returning id",
                                                               $job->getName(), '-infinity', $k, count($prevIds) ? '{' . join(',', $prevIds) . '}' : null
                                         );
                        $ids = [ $id ];
                        if(isset($param))
                            exec_query('insert into job_runner_job_param(job_runner_job_id, param) values(?,?)', $id, json_encode($param));
                    }else{
                        foreach($v as $k1 => $j){
                            $id = fetch_value("insert into job_runner_job(name, run_after, pos, subpos,  depends_on)
                                                                   values(?,    ?,?,?,   ?::bigint[])
                                                                   returning id",
                                                                   $job->getName(), '-infinity', $k, $k1, count($prevIds) ? '{' . join(',', $prevIds) . '}' : null
                                             );
                            $ids[] = $id;
                            if(isset($param))
                                exec_query('insert into job_runner_job_param(job_runner_job_id, param) values(?,?)', $id, json_encode($param));
                        }
                    }
                    foreach($prevIds as $id){
                        exec_query('update job_runner_job jr set dependants=?::bigint[] where id=?', (count($ids) ? '{' . join(',', $ids) . '}' : null), $id);
                    }
                    $prevIds = $ids;
                    $ids = [];
                }
            exec_query('commit');
        }

        function run(){
            while(1){
                exec_query('begin transaction');
                $row = fetch_row(
                'select * from job_runner_job jr where not jr.is_broken and jr.run_after<now() and not exists(select * from job_runner_job jr2, unnest(jr.depends_on) jdo(id) where jr2.id=jdo.id::bigint limit 1)                      for update skip locked 
                      limit 1'
                );

                if(!count($row)){
                    sleep(3);
                    continue;
                }
                $ref = $this->jobs[$row['name']]->getJobs()[$row['pos']];

                $fn = null;
                if(is_array($ref)){
                    $fn = $ref[$row['subpos']];
                }else{
                    $fn = $ref;
                }
                list($param, $next) = fetch_list('select jp.param, jp.next from job_runner_job_param jp where jp.job_runner_job_id=?', $row['id']);
                $param = json_decode($param,1);
                $next = json_decode($next,1);

                $jrv = new JobReturnValue(); $jrv->jobExecutor = $this; $jrv->row = $row;
                $rv = null;
                try{
                    $rv = $fn($jrv, $param, $next);
                }catch(Exception $e){
                    exec_query('update job_runner_job jr set last_run=now(), last_error=?, is_broken=true where jr.id=?', $e->getMessage(), $row['id']);
                }

                if(!$jrv->getResubmitInterval()){
                    exec_query('update job_runner_job_param jp set next=coalesce(next,\'{}\'::jsonb)|| ?::jsonb where jp.job_runner_job_id=any((select jr.dependants from job_runner_job jr where jr.id=?)::bigint[])', json_encode($rv), $row['id']);
                    exec_query('delete from job_runner_job where id=?', $row['id']);
                }else{
                    exec_query('update job_runner_job jp set run_after=run_after + make_interval(secs:=?) where id=?', $jrv->getResubmitInterval(), $row['id']);
                    exec_query('update job_runner_job_param jp set param=? where job_runner_job_id=?', json_encode($rv), $row['id']);
                }
                exec_query('commit');
            }
        }
    }

    $dbh = null;
    function getDbh(){
        global $dbh;
        if($dbh)
            return $dbh;

        $dbh = new \PDO('pgsql:zhost=localhost;port=5433;dbname=work', 'postgres','root');
        return $dbh;
    }

    function setDbh($pDbh){
        global $dbh;
        $dbh = $pDbh;
    }
    
    /*
      executes specified query with supplied param
      Last param can be a callback; if it is callback
      then it will be called with already executed, but not fetched statement
      If such callback is not specified then default callback will be used.
      The callback just returns affected rows
    */
    function exec_query($qry){
        $dbh = getDbh();

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
      return call_user_func_array( __NAMESPACE__ . '\\exec_query',
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
      return call_user_func_array( __NAMESPACE__ . '\\exec_query',
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
      return call_user_func_array(  __NAMESPACE__ . '\\exec_query' ,
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

    /*
     usage
      iterate_query("select * from tbl where id>?", 10, function($r){ print $r['id']; });
    */
    function iterate_query($query){
      $cb = func_get_args(); $cb = $cb[count($cb)-1];
      if(!is_callable($cb)){
         throw new Exception("Last passed parameter is not a callback!");
      }
      $param = array_slice(func_get_args(),0, func_num_args()-1);
      $dbh = getDbh();
      return call_user_func_array( __NAMESPACE__ .'\\exec_query',
                                        array_merge($param,
                                        [
                                            function($sth)use($cb){
                                               $cnt=0;
                                               while($row = $sth->fetch()){
                                                 $cb($row);
                                               }
                                            }
                                        ]
                                       )
                                     );
    }

    function fetch_value(){
      return @call_user_func_array( __NAMESPACE__ . '\\fetch_list', func_get_args())[0];
    }

}

