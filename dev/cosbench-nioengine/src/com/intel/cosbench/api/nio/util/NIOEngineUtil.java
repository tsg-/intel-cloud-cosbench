package com.intel.cosbench.api.nio.util;

import com.intel.cosbench.api.nio.client.NIOClient;
import com.intel.cosbench.api.nio.engine.NIOEngine;

public class NIOEngineUtil {

    public static NIOClient newClient(NIOEngine ioengine) {
    	
    	NIOClient ioclient = new NIOClient(ioengine.getConnPool());
    	
    	return ioclient;
    }
}
