<?php
include_once("job_manager.php");
#$dbh = new PDO('mysql:host=localhost;port=3306;dbname=jobs', 'root','');
$dbh = new PDO('pgsql:host=localhost;port=5433;dbname=work', 'postgres','root');
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
               function($param, &$ctx, $je){
                   print "In #2.2 param[name]={$param['name']} ctx[val]={$ctx['val']}\n";
                   $ctx['val']++;
                   if($ctx['val']==6){
                       $je->execute('sendmail', [ 'to' => 'lala@dodo.com', 'subject' => 'Just subject', 'body' => $param['name'] ]); 
                   }
                   if($ctx['val']<10){
                      return "CONTINUE";
                   }
               }
             ]
    )
    ->submit(function($param, &$ctx){
                   print "In #3 param[name]={$param['name']} ctx[val]={$ctx['val']}\n";
                   $ctx['val']++;
            });
$je->add($job);

$sendMailJob = new Job('sendmail');
$sendMailJob->submit( function($param, &$ctx){
                          print "********************** Sending mail to {$param['to']}\n";
                          $ctx['sended']=true;
                    })
            ->submit( function($param, &$ctx){
                      if($ctx['sended']){
                         print "########################### Sended!\n";
                      }
            });

$je->add($sendMailJob);

$je->execute("RUN#1", [ "path" => 1, "name"=>'Name'.time() ]);	
$je->run();
