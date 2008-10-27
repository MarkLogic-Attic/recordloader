/*
 * Copyright (c)2005-2008 Mark Logic Corporation
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

package com.marklogic.ps;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.marklogic.recordloader.Configuration;
import com.marklogic.recordloader.FatalException;
import com.marklogic.recordloader.InputHandlerInterface;
import com.marklogic.recordloader.LoaderException;
import com.marklogic.recordloader.Monitor;

/**
 * @author Michael Blakeley, <michael.blakeley@marklogic.com>
 * 
 */

public class RecordLoader {

    private static final String SIMPLE_NAME = RecordLoader.class
            .getSimpleName();

    public static final String VERSION = "2008-10-26.1";

    public static final String NAME = RecordLoader.class.getName();

    private static SimpleLogger logger = SimpleLogger.getSimpleLogger();

    private class CallerBlocksPolicy implements RejectedExecutionHandler {

        private BlockingQueue<Runnable> queue;

        /*
         * (non-Javadoc)
         * 
         * @see java.util.concurrent.RejectedExecutionHandler#rejectedExecution(java.lang.Runnable,
         *      java.util.concurrent.ThreadPoolExecutor)
         */
        public void rejectedExecution(Runnable r,
                ThreadPoolExecutor executor) {
            if (null == queue) {
                queue = executor.getQueue();
            }
            try {
                // block until space becomes available
                queue.put(r);
            } catch (InterruptedException e) {
                // someone is trying to interrupt us
                throw new RejectedExecutionException(e);
            }
        }

    }

    private Configuration config = new Configuration();

    private ArrayList<String> inputs = new ArrayList<String>();

    private Monitor monitor;

    private ThreadPoolExecutor pool;

    public RecordLoader(String[] args) throws IOException,
            URISyntaxException {
        configureFiles(Arrays.asList(args).iterator());

        initConfiguration();
        logger.info("client hostname = "
                + InetAddress.getLocalHost().getHostName());
        logger.info(printVersion());
    }

    /**
     * @throws URISyntaxException
     * 
     */
    private void initConfiguration() throws URISyntaxException {
        config.setLogger(logger);
        
        // use system properties as a basis
        // this allows any number of properties at the command-line,
        // using -DPROPNAME=foo
        // as a result, we no longer need any args: default to stdin
        config.load(System.getProperties());

        // now that we have a base configuration, we can bootstrap into the
        // correct modularized configuration
        // this should only be called once, in a single-threaded main() context
        try {
            String configClassName = config.getConfigurationClassName();
            logger.info("Configuration is " + configClassName);
            Class<? extends Configuration> configurationClass = Class
                    .forName(configClassName, true,
                            ClassLoader.getSystemClassLoader())
                    .asSubclass(Configuration.class);
            Constructor<? extends Configuration> configurationConstructor = configurationClass
                    .getConstructor(new Class[] {});
            Properties props = config.getProperties();
            config = configurationConstructor.newInstance(new Object[0]);
            // must pass properties to the new instance
            // TODO should this be a different method than load()?
            config.load(props);
        } catch (Exception e) {
            throw new FatalException(e);
        }

        // now the configuration is final
        config.configure();
    }

    public static void main(String[] args) throws Exception {
        System.err.println(printVersion());
        new RecordLoader(args).run();
    }

    /**
     * 
     */
    protected static String printVersion() {
        return SIMPLE_NAME + " starting, version " + VERSION + " on "
                + System.getProperty("java.runtime.name");
    }

    private void run() throws LoaderException, SecurityException,
            IllegalArgumentException {

        // if START_ID was supplied, run single-threaded until found
        int threadCount = config.getThreadCount();
        String startId = null;
        if (config.hasStartId()) {
            startId = config.getStartId();
            logger.warning("will single-thread until start-id \""
                    + startId + "\" is reached");
            threadCount = 1;
        }
        logger.info("thread count = " + threadCount);

        pool = new ThreadPoolExecutor(config.getThreadCount(), config
                .getThreadCount(), config.getKeepAliveSeconds(),
                TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(config
                        .getQueueCapacity()), new CallerBlocksPolicy());
        monitor = new Monitor(config, pool, Thread.currentThread());

        try {
            monitor.start();

            runInputHandler();

            pool.shutdown();

            while (monitor.isAlive()) {
                try {
                    monitor.join();
                } catch (InterruptedException e) {
                    if (pool.isTerminated()) {
                        logger.info("pool has terminated, exiting");
                    } else {
                        logger.logException("interrupted", e);
                    }
                }
            }

            try {
                pool.awaitTermination(60, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                if (null != monitor && monitor.isAlive()) {
                    logger.logException(e);
                }
                // harmless - this means the monitor wants to exit
                // if anything went wrong, the monitor will log it
                logger.warning("exiting due to interrupt");
            }
        } finally {
            if (null != pool) {
                pool.shutdownNow();
            }
            if (null != monitor && monitor.isAlive()) {
                monitor.halt();
            }
        }
    }

    /**
     * @throws LoaderException
     * 
     */
    private synchronized void runInputHandler() throws LoaderException {
        // this should only be called once, in a single-threaded context
        InputHandlerInterface inputHandler = null;
        try {
            Constructor<? extends InputHandlerInterface> handlerConstructor;
            String handlerClassName = config.getInputHandlerClassName();
            logger.info("input handler = " + handlerClassName);
            Class<? extends InputHandlerInterface> handlerClass = Class
                    .forName(handlerClassName, true,
                            ClassLoader.getSystemClassLoader())
                    .asSubclass(InputHandlerInterface.class);
            handlerConstructor = handlerClass
                    .getConstructor(new Class[] {});
            inputHandler = handlerConstructor.newInstance();
            inputHandler.setLogger(logger);
            inputHandler.setConfiguration(config);
            inputHandler.setPool(pool);
            inputHandler.setMonitor(monitor);
            logger.info("inputs.size = " + inputs.size());
            inputHandler.setInputs(inputs.toArray(new String[0]));
        } catch (Exception e) {
            // if anything went wrong in setup, it was fatal
            throw new FatalException(e);
        }
        inputHandler.run();
    }

    /**
     * @param iter
     * @throws IOException
     * @throws FileNotFoundException
     */
    private void configureFiles(Iterator<String> iter)
            throws IOException, FileNotFoundException {
        File file = null;
        String arg = null;
        while (iter.hasNext()) {
            arg = iter.next();
            if (!arg.endsWith(".properties")) {
                inputs.add(arg);
                continue;
            }

            // this will override existing properties
            file = new File(arg);
            if (!file.exists()) {
                logger.warning("skipping " + arg
                        + ": file does not exist.");
                continue;
            }
            if (!file.canRead()) {
                logger.warning("skipping " + arg
                        + ": file cannot be read.");
                continue;
            }
            logger.info("processing: " + arg);
            config.load(new FileInputStream(file));
        }
    }

}
