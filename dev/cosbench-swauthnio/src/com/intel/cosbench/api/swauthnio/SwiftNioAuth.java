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
import com.intel.cosbench.api.context.Context;
import com.intel.cosbench.api.nio.client.NIOClient;
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
    private ResponseValidator validator;

    /* account info */
    private String url;
    private String username;
    private String password;

    /* connection setting */
    private int timeout;

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
        	nioclient = (NIOClient)ioengine.newClient();
        	validator = new SwiftAuthResponseValidator();
        	nioclient.setValidator(validator);
            logger.info("swift client has been initialized");
        }else {
            logger.error("swift i/o engine is not correctly initialized, please check it first.");
        }
        
        logger.debug("swauth client has been initialized");
    }

    @Override
    public void dispose() {
        super.dispose();
    }

    @Override
    public Context login() {
        super.login();
        try {
           	// construct request.
        		System.out.println("SwAuthNio: url=" + url);
                URI uri = URI.create(url);
                
                BasicHttpRequest method = nioclient.makeHttpGet(uri.getPath());
                method.setHeader(X_STORAGE_USER, username);
                method.setHeader(X_STORAGE_PASS, password);  
                
                // issue request.
        		HttpHost target = new HttpHost(uri.getHost(), uri.getPort(), "http");

        		if(nioclient == null)
        		{
        			System.err.println("nio client is not initialized yet!");
        			return null;
        		}
                nioclient.GET_withWait(target, method);
                System.out.println("Request is issued!");
        } catch (SocketTimeoutException ste) {
            throw new AuthTimeoutException(ste);
        } catch (ConnectTimeoutException cte) {
            throw new AuthTimeoutException(cte);
        } catch (InterruptedIOException ie) {
            throw new AuthInterruptedException(ie);
        } catch (SwiftAuthClientException se) {
            throw new AuthException(se.getMessage(), se);
        } catch (Exception e) {
            throw new AuthException(e);
        }
        return validator.getResults();
    }

}
