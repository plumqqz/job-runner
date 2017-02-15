<?php
namespace JobRunner{
    class JobSubmitException extents Exception {};

    class Job{
        private $jobs = [];
        private $name;

        function __construct($name){
            $this->name = $name;
            return $this;
        }
        function submit($param){
            if(is_callable($param)){
                $jobs[] = $param;
            }elseif(is_array($param)){
                foreach($param as $p){
                    if(!is_callable($p))
                        throw new JobSubmitException("Passed array has non-function element");
                }
                $jobs[] = $param;
            }else{
                throw new JobSubmitException("Passed param neither function nor array of functions");
            }
            return $this;
        }
    }
}

