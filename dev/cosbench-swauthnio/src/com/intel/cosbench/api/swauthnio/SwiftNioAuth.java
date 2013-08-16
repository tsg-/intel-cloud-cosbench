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

package com.intel.cosbench.api.swauthnio;

import static com.intel.cosbench.client.swauthnio.SwiftAuthConstants.*;

import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.net.URI;

import org.apache.http.HttpHost;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.message.BasicHttpRequest;

import com.intel.cosbench.api.auth.*;
import com.intel.cosbench.api.context.ExecContext;
import com.intel.cosbench.api.nio.client.NIOClient;
import com.intel.cosbench.api.nio.engine.NIOEngine;
import com.intel.cosbench.api.nio.util.NIOEngineUtil;
import com.intel.cosbench.api.validator.ResponseValidator;
import com.intel.cosbench.client.swauthnio.*;
import com.intel.cosbench.config.Config;
import com.intel.cosbench.log.Logger;

/**
 * This class encapsulates a Swauth implementation for the Auth-API.
 * 
 * @author ywang19
 * 
 */
class SwiftNioAuth extends NoneAuth {
    private NIOClient nioclient;

    /* account info */
    private String url;
    private String username;
    private String password;

    /* connection setting */
    private int timeout;
    
    private ResponseValidator validator;

    public SwiftNioAuth() {
        /* empty */
    }

    @Override
    public void init(Config config, Logger logger) {
        super.init(config, logger);

        url = config.get(AUTH_URL_KEY,
                config.get(AUTH_URL_ALTKEY, URL_DEFAULT));

        username = config.get(AUTH_USERNAME_KEY, AUTH_USERNAME_DEFAULT);
        password = config.get(AUTH_PASSWORD_KEY, AUTH_PASSWORD_DEFAULT);
        timeout = config.getInt(CONN_TIMEOUT_KEY, CONN_TIMEOUT_DEFAULT);

        parms.put(AUTH_URL_KEY, url);
        parms.put(AUTH_USERNAME_KEY, username);
        parms.put(AUTH_PASSWORD_KEY, password);
        parms.put(CONN_TIMEOUT_KEY, timeout);

        logger.debug("using auth config: {}", parms);

        if(ioengine != null) {
        	nioclient = NIOEngineUtil.newClient((NIOEngine)ioengine);
        	validator = new SwiftAuthResponseValidator();
        	nioclient.setValidator(validator);
        	logger.info("swauth client has been initialized");
        }else {
            logger.error("swauth i/o engine is not correctly initialized, please check it first.");
        }
        
        logger.debug("swauth client has been initialized");
    }

    @Override
    public void dispose() {
        super.dispose();
    }

    @Override
    public ExecContext login() {
        super.login();

        try {
           	// construct request.
        		logger.debug("Auth: url={}", url);
                URI uri = URI.create(url);
                
                BasicHttpRequest method = nioclient.makeHttpGet(uri.getPath());
                method.setHeader(X_STORAGE_USER, username);
                method.setHeader(X_STORAGE_PASS, password);  
                
                // issue request.
        		HttpHost target = new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme());

        		if(nioclient == null)
        		{
        			logger.error("nio client is not initialized yet!");
        			return null;
        		}
        		
        		ExecContext context = new ExecContext(target, method, null, "login", 0);
        		nioclient.GETorHEAD(target, method, context, true);
//        		validator.validate(response, context);

        		return context;
        } catch (SocketTimeoutException ste) {
        	logger.error("Auth: SocketTimeoutException");
            throw new AuthTimeoutException(ste);
        } catch (ConnectTimeoutException cte) {
        	logger.error("Auth: ConnectTimeoutException");
            throw new AuthTimeoutException(cte);
        } catch (InterruptedIOException ie) {
        	logger.error("Auth: InterruptedException");
            throw new AuthInterruptedException(ie);
        } catch (SwiftAuthClientException se) {
        	logger.error("Auth: Swift Authentication Client Exception");
            throw new AuthException(se.getMessage(), se);
        } catch (Exception e) {
        	logger.error("Auth: Unknown Exception.");
            throw new AuthException(e);
        }
    }

}
