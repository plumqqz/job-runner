<?php
include_once("job_manager.php");
$dbh = new PDO('mysql:host=localhost;port=3306;dbname=jobs', 'root','');
$je = new JobExecutor();
$je->setDbh($dbh);

$job = new Job("RUN#1");
$job->submit(function($param, $ctx){
                   print "In #1\n";
             })
    ->submit([ function($param, $ctx){
                   print "In #2.1\n";        
                   throw new Exception('lala');
               },
               function($param, $ctx){
                   print "In #2.2\n";
               }
             ]
    )
    ->submit(function($param, $ctx){
                   print "In #3\n";
            });
$je->add($job);

$je->execute("RUN#1", [ "path" => 1 ]);	
$je->run();
