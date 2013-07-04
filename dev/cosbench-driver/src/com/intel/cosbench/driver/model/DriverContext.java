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

package com.intel.cosbench.driver.model;

import com.intel.cosbench.log.LogFactory;
import com.intel.cosbench.log.LogManager;
import com.intel.cosbench.log.Logger;
import com.intel.cosbench.model.DriverInfo;

/**
 * This class encapsulates the configurations in driver.conf.
 * 
 * @author ywang19, qzheng7
 * 
 */
public class DriverContext implements DriverInfo {

    private String name;
    private String url;
    
    // add IO Engine parameters
    private String ioengine;
    private int channels;
    private int concurrency;

    private boolean status;
    
    public String getIoengine() {
		return ioengine;
	}

	public void setIoengine(String ioengine) {
		this.ioengine = ioengine;
	}

	public int getChannels() {
		return channels;
	}

	public void setChannels(int channels) {
		this.channels = channels;
	}

	public int getConcurrency() {
		return concurrency;
	}

	public void setConcurrency(int concurrency) {
		this.concurrency = concurrency;
	}

	public DriverContext() {
        /* empty */
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public boolean getStatus() {
    	return status;
    }
    
    public void setStatus(boolean status) {
    	this.status = status;
    }
    
}
