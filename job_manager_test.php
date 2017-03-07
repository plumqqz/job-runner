<?php
include_once("job_manager.php");
#$dbh = new PDO('mysql:host=localhost;port=3306;dbname=jobs', 'root','');
$dbh = new PDO('pgsql:host=localhost;port=5433;dbname=work', 'postgres','root');
$je = new JobExecutor($dbh);
#$je->setDbh($dbh);

$job = new Job("RUN#1");
$job->submit(function($param, &$ctx){
                   $ctx['val']=1;
                   print "In #1 param[name]={$param['name']} ctx[val]={$ctx['val']}\n";
             })
    ->submit([ function($param, &$ctx){
                   print "In #2.1 param[name]={$param['name']} ctx[val]={$ctx['val']}\n";        
                   if(!isset($ctx['val1']))
                     $ctx['val1']=0;
                   $ctx['val1']++;
                   if($ctx['val1']<40)
                        return [ 'run_after' => 1 ];
                   return null;
               },
               function($param, &$ctx, $je){
                   print "In #2.2 param[name]={$param['name']} ctx[valx]={$ctx['valx']}\n";
                   if(!isset($ctx['valx']))
                     $ctx['valx']=0;
                   $ctx['valx']++;
                   if($ctx['valx']==4){
                      $je->execute('sendmail', [ 'to' => 'lala@dodo.com', 'subject' => 'Just subject', 'body' => $param['name'] ]); 
                   }
                   $je->exec_query('insert into cnt(val) values(1)');
                   if($ctx['valx']<6){
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

$payoutJob = new Job("user-payout");
$payoutJob->submit( function($param, &$ctx, $je){
                             print "Payout to user {$param['to']}\n";
                    });
$je->add($payoutJob);

$sendMailJob = new Job('sendmail');
$sendMailJob->submit( function($param, &$ctx){
                          print "********************** Sending mail to {$param['to']}\n";
                          $ctx['sended']=true;
                    })
            ->submit( function($param, &$ctx, $je){
                      if($ctx['sended']){
                         print "########################### Sended!\n";
                      }
            });

$je->add($sendMailJob);

$je->execute("RUN#1", [ "path" => 1, "name"=>'Name'.time() . getmypid() ]);	
$onceJob = new Job("once");
$je->run();
