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

package com.intel.cosbench.api.storage;

import static com.intel.cosbench.api.storage.StorageConstants.*;

import java.io.*;
import java.util.*;

import org.apache.http.HttpResponse;

import com.intel.cosbench.api.context.*;
import com.intel.cosbench.api.ioengine.IOEngineAPI;
import com.intel.cosbench.api.stats.StatsListener;
import com.intel.cosbench.api.validator.ResponseValidator;
import com.intel.cosbench.config.Config;
import com.intel.cosbench.log.Logger;

/**
 * This class encapsulates one none storage system which is used if no any other
 * storage system is assigned.
 * 
 * @author ywang19, qzheng7
 * 
 */
public class NoneStorage implements StorageAPI {

    public static final String API_TYPE = "none";

    protected volatile Context parms;
    protected Logger logger;
    
    protected IOEngineAPI ioengine;
    protected StatsListener listener;
    protected ResponseValidator validator;

    /* configurations */
    private boolean logging = false; // enable logging

    public NoneStorage() {
        /* empty */
    }
    
    @Override
    public IOEngineAPI initIOEngine(IOEngineAPI ioengine) {
    	return this.ioengine = ioengine;
    }
    
    @Override
    public void init(Config config, Logger logger) {
        this.logger = logger;
        this.parms = new Context();
		
        if(config != null)
        	logging = config.getBoolean(LOGGING_KEY, LOGGING_DEFAULT);
        /* register all parameters */
        parms.put(LOGGING_KEY, logging);
    }

    @Override
    public void setAuthContext(ExecContext info) {
        /* empty */
    	parms.putAll(info);
    }

    @Override
    public void dispose() {
        /* empty */
    }

    @Override
    public Context getParms() {
        return parms;
    }

    @Override
    public void abort() {
        /* empty */
    }

    @Override
    public void getObject(final String opType, String container, String object, Config config) throws StorageException, IOException {
        if (logging)
            logger.info("performing GET at /{}/{}", container, object);
//        return new ByteArrayInputStream(new byte[] {});
    }

    @Override
    public void createContainer(final String opType, String container, Config config) throws StorageException, IOException {
        if (logging)
            logger.info("performing PUT at /{}", container);
    }

    @Deprecated
    public void createObject(final String opType, String container, String object, byte[] data,
            Config config) throws StorageException, IOException {
        if (logging)
            logger.info("performing PUT at /{}/{}", container, object);
    }

    @Override
    public void createObject(final String opType, String container, String object, InputStream data,
            long length, Config config) throws StorageException, IOException {
        if (logging)
            logger.info("performing PUT at /{}/{}", container, object);
    }

    @Override
    public void deleteContainer(final String opType, String container, Config config) throws IOException {
        if (logging)
            logger.info("performing DELETE at /{}", container);
    }

    @Override
    public void deleteObject(final String opType, String container, String object, Config config) throws StorageException, IOException {
        if (logging)
            logger.info("performing DELETE at /{}/{}", container, object);
    }

    @Override
    public void createMetadata(final String opType, String container, String object,
            Map<String, String> map, Config config) {
        if (logging)
            logger.info("performing POST at /{}/{}", container, object);
    }

    @Override
    public Map<String, String> getMetadata(final String opType, String container, String object,
            Config config) {
        if (logging)
            logger.info("performing HEAD at /{}/{}", container, object);
        return Collections.emptyMap();
    }

	@Override
	public void setListener(StatsListener listener) {
		this.listener = listener;		
	}

	@Override
	public void setValidator(ResponseValidator validator) {
		this.validator = validator;
	}

}
