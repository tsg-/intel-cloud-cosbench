package com.intel.cosbench.api.util;

import com.intel.cosbench.api.client.NIOClient;
import com.intel.cosbench.api.nioengine.NIOEngine;

public class NIOEngineUtil {

    public static NIOClient newClient(NIOEngine ioengine) {
    	
    	NIOClient ioclient = new NIOClient(ioengine.getConnPool());
    	
    	return ioclient;
    }
}
