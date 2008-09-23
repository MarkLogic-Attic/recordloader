/**
 * Copyright (c) 2008 Mark Logic Corporation. All rights reserved.
 */
package com.marklogic.recordloader;

import java.util.concurrent.ThreadPoolExecutor;

import com.marklogic.ps.SimpleLogger;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 *
 */
public interface InputHandlerInterface {

    /**
     * @throws LoaderException 
     * 
     */
    public void run() throws LoaderException;

    /**
     * @param _logger
     */
    public void setLogger(SimpleLogger _logger);

    /**
     * @param _config
     */
    public void setConfiguration(Configuration _config);

    /**
     * @param _inputs
     */
    public void setInputs(String[] _inputs);

    /**
     * @param pool
     */
    public void setPool(ThreadPoolExecutor pool);

    /**
     * @param monitor
     */
    public void setMonitor(Monitor monitor);

}
