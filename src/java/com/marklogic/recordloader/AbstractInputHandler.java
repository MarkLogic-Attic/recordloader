/**
 * Copyright (c) 2008 Mark Logic Corporation. All rights reserved.
 */
package com.marklogic.recordloader;

import java.nio.charset.CharsetDecoder;
import java.util.concurrent.ThreadPoolExecutor;

import com.marklogic.ps.SimpleLogger;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public abstract class AbstractInputHandler implements
        InputHandlerInterface {

    protected ThreadPoolExecutor pool;

    protected SimpleLogger logger;

    protected Configuration configuration;

    protected CharsetDecoder inputDecoder;

    protected LoaderFactory factory;

    protected Monitor monitor;

    protected String[] inputs;

    protected void getFactory() throws FatalException {
        try {
            factory = new LoaderFactory(monitor, inputDecoder,
                    configuration);
        } catch (SecurityException e) {
            throw new FatalException(e);
        } catch (NoSuchMethodException e) {
            throw new FatalException(e);
        } catch (ClassNotFoundException e) {
            throw new FatalException(e);
        }
    }

    public void setMonitor(Monitor _monitor) {
        monitor = _monitor;
    }

    public void setPool(ThreadPoolExecutor _pool) {
        pool = _pool;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.recordloader.InputHandlerInterface#run()
     */
    public abstract void run() throws LoaderException;

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.recordloader.InputHandlerInterface#setLogger(com.marklogic.ps.SimpleLogger)
     */
    public void setLogger(SimpleLogger _logger) {
        logger = _logger;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.recordloader.InputHandlerInterface#setConfiguration(com.marklogic.ps.recordloader.Configuration)
     */
    public void setConfiguration(Configuration _config) {
        configuration = _config;
        inputDecoder = configuration.getDecoder();
    }

    public void setInputs(String[] _inputs) {
        inputs = _inputs.clone();
    }

}
