/**
 * Copyright (c) 2008-2009 Mark Logic Corporation. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * The use of the Apache License does not indicate that this project is
 * affiliated with the Apache Software Foundation.
 */
package com.marklogic.recordloader;

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

    protected Configuration config;

    protected LoaderFactory factory;

    protected Monitor monitor;

    protected String[] inputs;

    protected void getFactory() throws FatalException {
        try {
            factory = new LoaderFactory(monitor, config);
        } catch (Exception e) {
            // this is crude, but all exceptions really must be fatal here
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
     * @see
     * com.marklogic.recordloader.InputHandlerInterface#setLogger(com.marklogic
     * .ps.SimpleLogger)
     */
    public void setLogger(SimpleLogger _logger) {
        logger = _logger;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.marklogic.recordloader.InputHandlerInterface#setConfiguration(com
     * .marklogic.ps.recordloader.Configuration)
     */
    public void setConfiguration(Configuration _config) {
        config = _config;
    }

    public void setInputs(String[] _inputs) {
        inputs = _inputs.clone();
    }

}
