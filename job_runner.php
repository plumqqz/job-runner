<?php

/*

create table job_runner_job(
  id bigserial primary key,
  name text not null,
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
        function add(Job $job, $param=null){
            $jobs[$job->getName()] = $job;
            exec_query('begin transaction');
                $prevIds = [];
                foreach($job->getJobs() as $v){
                    if(!is_array($v)){
                        $id = fetch_value("insert into job_runner_job(name, run_after , depends_on)
                                                               values(?,    ?,   ?)
                                                               returning id",
                                                               $job->getName(), '-infinity', '{' . join(',', $prevIds) . '}::bigint[]'
                                         );
                        $prevIds = [ $id ];
                    }else{
                        foreach($v as $j){
                            $id = fetch_value("insert into job_runner_job(name, run_after , depends_on)
                                                                   values(?,    ?,   ?)
                                                                   returning id",
                                                                   $job->getName(), '-infinity', '{' . join(',', $prevIds) . '}::bigint[]'
                                             );
                            $prevIds[] = [ $id ];
                        }
                    }

                }
            exec_query('commit');
        }
        function run(){
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
            throw new Exception("Cannot prepare query:" . $err);
        }

        if(!$sth->execute($param)){
            $err = $sth->errorInfo(); $err = $err[2];
            throw new Exception("Cannot exec query:" . $err);
        }

        return $cb($sth);
    }
    /*
      It's just a small wrapped about function above.
      The wrapped supplies the callbach which will fetch all rows and return array
    */
    function fetch_query($query){
      return call_user_func_array( 'exec_query',
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
      return call_user_func_array( 'exec_query',
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
      return call_user_func_array( 'exec_query',
                                    array_merge(func_get_args(),
                                    [
                                        function($sth){
                                           $res = $sth->fetch(PDO::FETCH_NUM);

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
      return call_user_func_array( 'exec_query',
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
      return @call_user_func_array( 'fetch_list', func_get_args())[0];
    }

}

