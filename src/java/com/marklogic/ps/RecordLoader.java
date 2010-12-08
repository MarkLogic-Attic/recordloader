/*
 * Copyright (c)2005-2010 Mark Logic Corporation
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
import java.lang.reflect.InvocationTargetException;
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
import java.util.logging.Level;

import com.marklogic.recordloader.Configuration;
import com.marklogic.recordloader.FatalException;
import com.marklogic.recordloader.InputHandlerInterface;
import com.marklogic.recordloader.LoaderException;
import com.marklogic.recordloader.Monitor;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */

public class RecordLoader {

    private static final String SIMPLE_NAME = RecordLoader.class
            .getSimpleName();

    public static final String VERSION = "2010-11-07.1";

    public static final String NAME = RecordLoader.class.getName();

    private static SimpleLogger logger = SimpleLogger.getSimpleLogger();

    private class CallerBlocksPolicy implements RejectedExecutionHandler {

        private BlockingQueue<Runnable> queue;

        /*
         * (non-Javadoc)
         * 
         * @see
         * java.util.concurrent.RejectedExecutionHandler#rejectedExecution(java
         * .lang.Runnable, java.util.concurrent.ThreadPoolExecutor)
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
                // reset interrupt status
                Thread.interrupted();
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
        logger.info(getVersionMessage());
    }

    public RecordLoader(Configuration configuration)
            throws URISyntaxException, IOException {
        this.config = configuration;
        initConfiguration();
        logger.info("client hostname = "
                + InetAddress.getLocalHost().getHostName());
        logger.info(getVersionMessage());
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
                    .forName(configClassName, true, getClassLoader())
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

    public static ClassLoader getClassLoader() {
        ClassLoader cl = null;
        try {
            cl = Thread.currentThread().getContextClassLoader();
        } catch (Throwable ex) {
            // the next test for null will take care of any errors
        }
        if (cl == null) {
            // No thread context ClassLoader, use ClassLoader of this class
            cl = RecordLoader.class.getClassLoader();
        }
        if (cl == null) {
            cl = ClassLoader.getSystemClassLoader();
        }
        return cl;
    }

    public static void main(String[] args) throws Exception {
        System.err.println(getVersionMessage());

        RecordLoader rl = null;
        try {
            rl = new RecordLoader(args);
            rl.run();
        } finally {
            rl.close();
        }
    }

    /**
     *
     */
    protected static String getVersionMessage() {
        return SIMPLE_NAME + " starting, version " + VERSION + " on "
                + System.getProperty("java.version") + " ("
                + System.getProperty("java.runtime.name") + ")" + " "
                + System.getProperty("file.encoding");
    }

    public void run() throws LoaderException, SecurityException,
            IllegalArgumentException, ClassNotFoundException,
            NoSuchMethodException {
        // if START_ID was supplied, run single-threaded until found
        int threadCount = config.getThreadCount();
        String startId = null;
        if (config.hasStartId()) {
            startId = config.getStartId();
            if (config.isStartIdMultiThreaded()) {
                logger
                        .warning("all threads will skip records until start-id \""
                                + startId + "\" is reached");

            } else {
                logger.warning("will single-thread until start-id \""
                        + startId + "\" is reached");
                threadCount = 1;
            }
        }
        logger.info("thread count = " + threadCount);

        Constructor<? extends InputHandlerInterface> inputHandlerConstructor = initInputHandlerConstructor();

        monitor = new Monitor(config, Thread.currentThread());

        while (true) {
            pool = new ThreadPoolExecutor(threadCount, threadCount,
                    config.getKeepAliveSeconds(), TimeUnit.SECONDS,
                    new ArrayBlockingQueue<Runnable>(config
                            .getQueueCapacity()),
                    new CallerBlocksPolicy());
            pool.prestartCoreThread();

            monitor.setPool(pool);
            if (config.isFirstLoop()) {
                monitor.start();
            }
            runInputHandler(inputHandlerConstructor);
            pool.shutdown();

            while (!pool.isTerminated()) {
                Thread.yield();
                try {
                    Thread.sleep(threadCount * Configuration.SLEEP_TIME);
                } catch (InterruptedException e) {
                    // reset interrupted status
                    Thread.interrupted();
                }
            }

            try {
                pool.awaitTermination(60, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // reset interrupted status
                Thread.interrupted();
                if (null != monitor && monitor.isAlive()) {
                    logger.logException(e);
                }
                // harmless - this means the monitor wants to exit
                // if anything went wrong, the monitor will log it
                logger
                        .warning("interrupted while waiting for pool termination");
            }

            if (!config.isLoopForever()) {
                break;
            }
            logger.log(config.isFirstLoop() ? Level.INFO : Level.FINE,
                    "looping...");
            config.setFirstLoop(false);
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                // reset interrupted status and ignore
                Thread.interrupted();
            }
        }
    }

    /**
     * 
     */
    private void halt() {
        if (null != pool) {
            pool.shutdownNow();
        }

        if (!config.isLoopForever()) {
            while (null != monitor && monitor.isAlive()) {
                try {
                    monitor.halt();
                    // wait for monitor to exit
                    monitor.join();
                } catch (InterruptedException e) {
                    // reset interrupted status and ignore
                    Thread.interrupted();
                }
            }
        }

        if (Thread.currentThread().isInterrupted()) {
            logger.fine("resetting thread status");
            Thread.interrupted();
        }
    }

    /**
     * @param _handlerConstructor
     * @throws LoaderException
     * 
     */
    private synchronized void runInputHandler(
            Constructor<? extends InputHandlerInterface> _handlerConstructor)
            throws LoaderException {
        // this should only be called once, in a single-threaded context
        InputHandlerInterface inputHandler = null;
        try {
            inputHandler = _handlerConstructor.newInstance();
            inputHandler.setLogger(logger);
            inputHandler.setConfiguration(config);
            inputHandler.setPool(pool);
            inputHandler.setMonitor(monitor);
            logger.log(config.isFirstLoop() ? Level.INFO : Level.FINE,
                    "inputs.size = " + inputs.size());
            inputHandler.setInputs(inputs.toArray(new String[0]));
        } catch (InvocationTargetException e) {
            // if anything went wrong in setup, it was fatal
            throw new FatalException(e);
        } catch (IllegalArgumentException e) {
            // if anything went wrong in setup, it was fatal
            throw new FatalException(e);
        } catch (InstantiationException e) {
            // if anything went wrong in setup, it was fatal
            throw new FatalException(e);
        } catch (IllegalAccessException e) {
            // if anything went wrong in setup, it was fatal
            throw new FatalException(e);
        }
        inputHandler.run();
    }

    /**
     * @return
     * @throws ClassNotFoundException
     * @throws NoSuchMethodException
     */
    private Constructor<? extends InputHandlerInterface> initInputHandlerConstructor()
            throws ClassNotFoundException, NoSuchMethodException {
        String handlerClassName = config.getInputHandlerClassName();
        logger.info("input handler = " + handlerClassName);
        Class<? extends InputHandlerInterface> handlerClass = Class
                .forName(handlerClassName, true, getClassLoader())
                .asSubclass(InputHandlerInterface.class);
        return handlerClass.getConstructor(new Class[] {});
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

    /**
     * 
     */
    public void close() {
        halt();
        if (null != config) {
            config.close();
        }
    }
}
