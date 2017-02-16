<?php
include_once "job_runner.php";
$job = new JobRunner\Job("consumer#1");
$job->submit(function($jrv, $p){
                            print "Here we are #1\n";
                            if($p){
                               $jrv->setResubmitInterval(0.05);
                               return 2;
                            }  
             })
    ->submit(function($jrv, $p){ print "Here we are #2\n"; print_r($p);  })
    ->submit([ 
               function ($jrv, $p) { print "Here we are #3.1\n"; print_r($p); },
               function ($jrv, $p) { print "Here we are #3.2\n"; print_r($p); }
            ]
           )
    ->submit(function($jrv, $p){ print "Here we are #4\n"; print_r($p); })
    ->submit(function($jrv, $p){ print "Here we are #5\n"; print_r($p); })
    ->end();
$je = new JobRunner\JobExecutor();
$je->add($job);
for($i=0;$i<1;$i++)
    $je->submit("consumer#1", [ [ "user_id" => 122 ] ]
               );
$je->run();
