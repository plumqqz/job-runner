<?php
include_once "job_runner.php";
$job = new JobRunner\Job("consumer#1");
$job->submit(function($jrv, $p){
                JobRunner\exec_query("update usr set email=?||'@mail.ru' where id=?", $p['user_id'], $p['user_id']);
             })
    ->submit(function($jrv, $p){ 
                JobRunner\exec_query("update usr set address='#' || ?||' Lenina ul' where id=?", $p['user_id'], $p['user_id']);
            })
    ->submit([ 
               function ($jrv, $p) {
                    print '---------';
                    print_r($p);
                    JobRunner\exec_query("update usr set age=coalesce(age,18)+1 where id=?", $p['user_id']);
               },
               function ($jrv, $p) { 
                    JobRunner\exec_query("update usr set age=coalesce(age,18)+1 where id=?", $p['user_id']);
               }
            ]
           )
    ->submit(function($jrv, $p){ print "Here we are #4\n"; })
    ->submit(function($jrv, $p){ print "Here we are #5\n"; })
    ->end();
$je = new JobRunner\JobExecutor();
$je->add($job);
for($i=0;$i<20000;$i++){
    $uid = JobRunner\fetch_value('insert into usr(email) values(\'v\') returning id');
    $uids = [ "user_id" => $uid ];
    $je->submit("consumer#1", $uids);
}
$je->run();
