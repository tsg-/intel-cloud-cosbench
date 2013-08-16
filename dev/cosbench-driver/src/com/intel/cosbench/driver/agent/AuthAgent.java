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

import java.util.Random;

import org.apache.commons.lang.math.RandomUtils;

import com.intel.cosbench.api.auth.*;
import com.intel.cosbench.api.context.Context;
import com.intel.cosbench.api.context.ExecContext;
import com.intel.cosbench.api.storage.StorageAPI;
import com.intel.cosbench.log.Logger;
import com.intel.cosbench.service.AbortedException;

class AuthAgent extends AbstractAgent {

    private int loginAttempts; // number of retries

    public AuthAgent() {
        /* empty */
    }

    public void setLoginAttempts(int loginAttempts) {
        this.loginAttempts = loginAttempts;
    }

    @Override
    protected void execute() {
        Logger logger = getMissionLogger();
        logger.debug("begin to login, will attempt {} times", loginAttempts);
        try {
            /*
             * Here we prepare the storage with the authentication object (e.g.
             * a user token) retrieved from an login operation. Some storage
             * implementations such as Swift and S3 do require such information,
             * others don't. But we will do this anyway.
             */
            StorageAPI storageApi = workerContext.getStorageApi();
            storageApi.setAuthContext(login());

        } catch (AuthInterruptedException ie) {
            throw new AbortedException();
        } catch (AuthBadException be) {
            logger.error("bad username and password", be);
            throw new AgentException(); // mark error
        } catch (AuthException e) {
            if (loginAttempts == 1)
                logger.error("unable to login", e);
            else
                logger.error("still unable to login", e);
            logger.error("fail to login with {} attempt(s)", loginAttempts);
            throw new AgentException(); // mark error
        } finally {
            AuthAPI authApi = workerContext.getAuthApi();
            authApi.dispose(); // early dispose connection
        }
        int idx = workerContext.getIndex();
        logger.debug("worker {} has been successfully authed", idx);
    }

    private ExecContext login() {
        Logger logger = getMissionLogger();
        AuthAPI authApi = workerContext.getAuthApi();
        int attempts = 0;
        while (attempts++ < loginAttempts - 1)
            try {
                return authApi.login();
            } catch (AuthInterruptedException ie) {
            	System.err.println("AuthError");
                throw ie; // do not mask this one
            } catch (AuthBadException be) {
            	System.err.println("AuthError");
                throw be; // do not mask this one, either
            } catch (AuthException e) {
            	System.err.println("AuthError");
                logger.error("unable to login, will try again later", e);
                sleepForSometime();
            }
        return authApi.login(); // the very last attempt!
    }

    private void sleepForSometime() {
        Random random = workerContext.getRandom();
        int time = (10 + RandomUtils.nextInt(random, 40)) * 100;
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            throw new AuthInterruptedException(e);
        }
        Logger logger = getMissionLogger();
        logger.debug("has waited for {} ms, will make a new attempt", time);
    }

}
