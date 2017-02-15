<?php
include_once "job_runner.php";
$job = new JobRunner\Job("consumer#1");
$job->submit(function(){ print "Here we are #1\n"; return [ 'result' => 1 ]; })
    ->submit(function($p){ print "Here we are #2\n"; return [ 'result' => $p['result']+1 ]; })
    ->submit([ 
               function ($p) { print "Here we are #3.1\n"; return [ 'result' => $p['result']+1 ];},
               function ($p) { print "Here we are #3.1\n"; return [ 'result' => $p['result']+1 ];}
            ]
           )
    ->submit(function($p){ print "Here we are #4\n"; return [ 'result' => $p[0]['result']+1 ]; })
    ->end();
$je = new JobRunner\JobExecutor();
$je->add($job);
$je->run();
