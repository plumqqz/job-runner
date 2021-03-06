<?php
include_once("job_manager.php");
#$dbh = new PDO('mysql:host=localhost;port=3306;dbname=jobs', 'root','');
$dbh = new PDO('pgsql:host=localhost;port=5433;dbname=work', 'postgres','root');
$je = new JobExecutor($dbh);
#$je->deleteJob($argv[1]);
#exit;
#$je->setDbh($dbh);

$jobWaiter = new Job("Waiter");
$jobWaiter->submit(function($param, &$ctx){
                  if(@$ctx['waiter_done']){
                      return null;
                  }
                  if(!@$ctx['wait']){
                      $ctx['wait']=1;
                      print "--------------------------------->wait\n";
                      return 1;
                  }
                  return ["user-payout", ['user'=>1, 'rnd' => rand() ], [], 0 ];
            })->submit(function($param, &$ctx, $je){
                if(@$ctx['waiter_done']) {
                    $ctx['waiter_done']++;
                    print "#################################!\n";
                    return null;
                }
                print "=================================>waited!\n";
                print ">> ctx[payed]={$ctx['payed']}<<\n";
                $ctx=[];
                $ctx['waiter_done']=1;
                #$je->execute( 'Waiter', ['time' => time().getmypid()], ['waiter_done'=>1], 0, [], $je->listDependantSteps());
                return null;
            });

$job = new Job("RUN#1");
$job->submit(function($param, &$ctx){
                   $ctx['val']=1;
                   $ctx['val1']=0;
                   $ctx['valx']=0;
                   print "In #1 param[name]={$param['name']} ctx[val]={$ctx['val']}\n";
                   return [ 'Waiter', ['time' => time()],[],0];
                   return null;
             })
    ->submit([ function($param, &$ctx){
                   print "In #2.1 param[name]={$param['name']} ctx[val]={$ctx['val']}\n";        
                   print ">>>!!! {$ctx['waiter_done']} !!!<<<\n";
                   if(!isset($ctx['val1']))
                     $ctx['val1']=0;
                   $ctx['val1']=$ctx['val1']+1;
                   if($ctx['val1']<4)
                        return 0.1;
                   return null;
               },
               function($param, &$ctx, $je){
                   print "In #2.2 param[name]={$param['name']} ctx[valx]={$ctx['valx']}\n";
                   if(!isset($ctx['valx']))
                     $ctx['valx']=0;
                   $ctx['valx']++;
                   if($ctx['valx']==4){
                      #$je->execute('sendmail', [ 'to' => 'lala@dodo.com', 'subject' => 'Just subject', 'body' => $param['name'] ]); 
                      #$je->execute('.execute', ['name' => 'sendmail', 'param' => [ 'to' => 'lala@dodo.com', 'subject' => 'Just subject', 'body' => $param['name'] ] ]); 
                   }
                   $je->exec_query('insert into cnt(val) values(1)');
                   if($ctx['valx']<6){
                      return 0.1;
                   }
               }
             ]
    )
    ->submit(function($param, &$ctx){
                   print "In #3 param[name]={$param['name']} ctx[val]={$ctx['val']}\n";
                   $ctx['val']++;
            });
$job->setCallback(function($p,&$ctx,$je){
    if(0 && isset($ctx['val1']))
      throw new Exception("Broke job!");
});
$je->add($job);

$payoutJob = new Job("user-payout");
$payoutJob->submit( function($param, &$ctx, $je){
                             print "Payout to user {$param['to']}\n";
                             $ctx['payed']=1;
                    });
$je->add($payoutJob);
$je->add($jobWaiter);

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
#$je->resumeJob(120378);
    $je->execute("RUN#1", [ "path" => 1, "name"=>'Name'.time() . getmypid() ]);	
$je->run(['%'],function(){ print "idle ";});
exit;

if(!count($je->listNotEndedJobs('RUN#1'))){
    $je->execute("RUN#1", [ "path" => 1, "name"=>'Name'.time() . getmypid() ]);	
}else{
    $je->resumeJob($argv[1]);
}
$onceJob = new Job("once");
#$je->run(['RUN#1', 'sendmail', '.execute']);
