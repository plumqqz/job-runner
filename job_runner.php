<?php

/*

create table job_runner_job(
  id bigserial primary key,
  name text not null,
  pos integer,
  subpos integer,
  run_after timestamptz,
  depends_on decimal[]  
);
create index on job_runner_job(name, run_after);

create table job_runner_job_param(
  job_runner_job_id bigint primary key references job_runner_job(id),
  param json
);

*/

namespace JobRunner{
    class JobSubmitException extends \Exception {};

    class Job{
        private $jobs = [];
        private $name;

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

    class JobExecutor{
        private $jobs = [];
        function add(Job $job){
            $this->jobs[$job->getName()] = $job;
        }
        function submit($jobName, $param = null){
            exec_query('begin transaction');
                $job = $this->jobs[$jobName];
                $prevIds = []; $ids = [];
                foreach($job->getJobs() as $k=>$v){
                    if(!is_array($v)){
                        $id = fetch_value("insert into job_runner_job(name, run_after, pos, depends_on)
                                                               values(?,    ?, ?,  ?::bigint[])
                                                               returning id",
                                                               $job->getName(), '-infinity', $k, count($prevIds) ? '{' . join(',', $prevIds) . '}' : null
                                         );
                        $ids = [ $id ];
                    }else{
                        foreach($v as $k1 => $j){
                            $id = fetch_value("insert into job_runner_job(name, run_after, pos, subpos,  depends_on)
                                                                   values(?,    ?,?,?,   ?::bigint[])
                                                                   returning id",
                                                                   $job->getName(), '-infinity', $k, $k1, count($prevIds) ? '{' . join(',', $prevIds) . '}' : null
                                             );
                            $ids[] = $id;
                        }
                    }
                    if(count($prevIds)==0){
                        foreach($ids as $id){
                            exec_query('insert into job_runner_job_param(job_runner_job_id, param) values(?,?)', $id, json_encode($param));
                        }
                    }
                    $prevIds = $ids;
                    $ids = [];
                }
            exec_query('commit');
        }

        function run(){
            while(1){
                exec_query('begin transaction');
                $row = fetch_row('select * from job_runner_job jr where jr.run_after<now() and not exists(select * from job_runner_job jr2 where jr2.id=any(jr.depends_on)) for update skip locked');
                if(!$row){
                    sleep(3);
                }
                $ref = $this->jobs[$row['name']]->getJobs()[$row['pos']];
                print_r($ref);
                $fn = null;
                if(is_array($ref)){
                    $fn = $ref[$row['subpos']];
                }else{
                    $fn = $ref;
                }
                $fn();
                exec_query('delete from job_runner_job where id=?', $row['id']);
                exec_query('commit');
            }
        }
    }

    function getDbh(){
        static $dbh;
        if($dbh)
            return $dbh;

        $dbh = new \PDO('pgsql:host=localhost;port=5433;dbname=work', 'postgres','root');
        return $dbh;
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
      return call_user_func_array( __NAMESPACE__ . '\\fetch_list', func_get_args())[0];
    }

}

