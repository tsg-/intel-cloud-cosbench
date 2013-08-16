/** 
 
Copyright 2013 Intel Corporation, All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. 
*/ 

package com.intel.cosbench.driver.agent;

import java.util.*;

import com.intel.cosbench.driver.model.*;
import com.intel.cosbench.driver.util.OperationPicker;
import com.intel.cosbench.service.AbortedException;

public class WorkAgent extends AbstractAgent {

    private OperationPicker operationPicker;
    private OperatorRegistry operatorRegistry;

    public WorkAgent() {
        /* empty */
    }

    @Override
    public void setWorkerContext(WorkerContext workerContext) {
        super.setWorkerContext(workerContext);
    }

    public void setOperationPicker(OperationPicker operationPicker) {
        this.operationPicker = operationPicker;
    }

    public void setOperatorRegistry(OperatorRegistry operatorRegistry) {
        this.operatorRegistry = operatorRegistry;
        workerContext.setOperatorRegistry(operatorRegistry);
    }

    @Override
    protected void execute() {
    	workerContext.init();
        doWork(); // launch work
    }

    private void doWork() {
    	workerContext.getSnapshot();
    	
        while (workerContext.isRunning())
            try {
                performOperation();
            } catch (AbortedException ae) {
            	if(workerContext.getStats().hasSamples())
            		workerContext.getStats().doSummary();
//                workerContext.getStats().finished();
            }
        
        workerContext.getSnapshot();
    }

//    private void waitForCompletion(long interval) {
//    	workerContext.waitForCompletion(interval);    	
//    }
    
    private void performOperation() {
        Random random = workerContext.getRandom();
        String op = operationPicker.pickOperation(random);
        OperatorContext context = operatorRegistry.getOperator(op);
        workerContext.updateStats();
        context.getOperator().operate(workerContext);
    }

}
