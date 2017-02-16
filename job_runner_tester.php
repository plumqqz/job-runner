<?php
include_once "job_runner.php";
$job = new JobRunner\Job("consumer#1");
$job->submit(function($jrv, $p){ print "Here we are #1\n"; print_r($p); return [ 'result' => 1 ]; })
    ->submit(function($jrv, $p){ print "Here we are #2\n"; print_r($p); return [ 'result' => $p['result']+1 ]; })
    ->submit([ 
               function ($jrv, $p) { print "Here we are #3.1\n"; print_r($p); return [ 'result1' => $p['result']+1 ];},
               function ($jrv, $p) { print "Here we are #3.2\n"; print_r($p); return [ 'result2' => $p['result']+1 ];}
            ]
           )
    ->submit(function($jrv, $p){ print "Here we are #4\n"; print_r($p); return [ 'result' => $p['result1']+$p['result2']+1 ]; })
    ->submit(function($jrv, $p){ print "Here we are #5\n"; print_r($p); })
    ->end();
$je = new JobRunner\JobExecutor();
$je->add($job);
#for($i=0;$i<20000;$i++)
#    $je->submit("consumer#1");
$je->run();
