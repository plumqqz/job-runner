<?php
include_once "job_runner.php";
$dbh = new \PDO('pgsql:host=localhost;port=5433;dbname=work', 'postgres','root');
JobRunner\setDbh($dbh);
$job = new JobRunner\Job("consumer#1");
$job->submit(function($jrv, $p){
                JobRunner\exec_query("update usr set email=?||'@mail.ru' where id=?", $p['user_id'], $p['user_id']);
                return [ 'result' => 1 ];
             })
    ->submit(function($jrv, $p, $prev){ 
                JobRunner\exec_query("update usr set address='#' || ?||' Lenina ul' where id=?", $p['user_id'], $p['user_id']);
                return [ 'result' => $prev['result']+1 ];
            })
    ->submit([ 
               function ($jrv, $p, $prev) {
                    JobRunner\exec_query("update usr set age=coalesce(age,18)+1 where id=?", $p['user_id']);
                    return [ 'result1' => $prev['result']+1 ];
               },
               function ($jrv, $p, $prev) { 
                    JobRunner\exec_query("update usr set age=coalesce(age,18)+1 where id=?", $p['user_id']);
                    return [ 'result2' => $prev['result']+1 ];
               }
            ]
           )
    ->submit( [
           function($jrv, $p, $prev){
                 JobRunner\exec_query("update usr set notes=coalesce(notes,'') || '1' where id=?", $p['user_id']);
                 return [ 'result1' => $prev['result1']+1 ];
           },
           function($jrv, $p, $prev){
                 JobRunner\exec_query("update usr set notes=coalesce(notes,'') || '2' where id=?", $p['user_id']);
                 return [ 'result2' => $prev['result2']+1 ];
           }
          ]
        )
    ->submit(function($jrv, $p, $prev){
                  print "Here we are #5:" .($prev['result1']+$prev['result2']);
          })
    ->end();
$je = new JobRunner\JobExecutor();
$je->add($job);

#$je->run();
#exit;
for($i=0;$i<50000;$i++){
    $uid = JobRunner\fetch_value('insert into usr(email) values(\'v\') returning id');
    $uids = [ "user_id" => $uid ];
    $je->submit("consumer#1", $uids);
}


