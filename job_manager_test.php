<?php
include_once("job_manager.php");
$dbh = new PDO('mysql:host=localhost;port=3306;dbname=jobs', 'root','');
$je = new JobExecutor();
$je->setDbh($dbh);

$job = new Job("RUN#1");
$job->submit(function($param, &$ctx){
                   $ctx['val']=1;
                   print "In #1 param[name]={$param['name']} ctx[val]={$ctx['val']}\n";
             })
    ->submit([ function($param, &$ctx){
                   print "In #2.1 param[name]={$param['name']} ctx[val]={$ctx['val']}\n";        
                   $ctx['val']++;
               },
               function($param, &$ctx){
                   print "In #2.2 param[name]={$param['name']} ctx[val]={$ctx['val']}\n";
                   $ctx['val']++;
               }
             ]
    )
    ->submit(function($param, &$ctx){
                   print "In #3 param[name]={$param['name']} ctx[val]={$ctx['val']}\n";
                   $ctx['val']++;
            });
$je->add($job);

$je->execute("RUN#1", [ "path" => 1, "name"=>'Name' ]);	
$je->run();
