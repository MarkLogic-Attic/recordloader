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
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

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

    public static final String VERSION = "2008-10-14.1";

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
        logger.info(printVersion());

        // is the environment healthy?
        checkEnvironment();

    }

    /**
     * @throws URISyntaxException
     * 
     */
    private void initConfiguration() throws URISyntaxException {
        // use system properties as a basis
        // this allows any number of properties at the command-line,
        // using -DPROPNAME=foo
        // as a result, we no longer need any args: default to stdin
        config.load(System.getProperties());
        config.setLogger(logger);

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
            config.load(props);
        } catch (Exception e) {
            throw new FatalException(e);
        }

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
                + System.getProperty("java.version");
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
        } catch (ClassNotFoundException e) {
            throw new FatalException(e);
        } catch (SecurityException e) {
            throw new FatalException(e);
        } catch (NoSuchMethodException e) {
            throw new FatalException(e);
        } catch (IllegalArgumentException e) {
            throw new FatalException(e);
        } catch (InstantiationException e) {
            throw new FatalException(e);
        } catch (IllegalAccessException e) {
            throw new FatalException(e);
        } catch (InvocationTargetException e) {
            throw new FatalException(e);
        }
        inputHandler.run();
    }

    /**
     * @throws IOException
     * 
     */
    private void checkEnvironment() throws IOException {
        // check the XPP3 version
        String resourceName = "META-INF/services/org.xmlpull.v1.XmlPullParserFactory";
        String versionSuffix = "_VERSION";
        String versionPrefix = "XPP3_";

        ClassLoader loader = RecordLoader.class.getClassLoader();
        if (null == loader) {
            logger
                    .warning("RecordLoader class loader is null - trying system class loader");
            loader = ClassLoader.getSystemClassLoader();
            if (null == loader) {
                throw new NullPointerException("null class loader");
            }
        }
        URL xppUrl = loader.getResource(resourceName);
        if (null == xppUrl) {
            throw new FatalException(
                    "Please configure your classpath to include"
                            + " XPP3 (version 1.1.4 or later).");
        }
        // the xppUrl should look something like...
        // jar:file:/foo/xpp3-1.1.4c.jar!/META-INF/services/org.xmlpull.v1.XmlPullParserFactory
        String proto = xppUrl.getProtocol();
        // TODO handle file protocol directly, too?
        if (!"jar".equals(proto)) {
            throw new FatalException("xppUrl protocol: " + proto);
        }
        // the file portion should look something like...
        // file:/foo/xpp3-1.1.4c.jar!/META-INF/services/org.xmlpull.v1.XmlPullParserFactory
        String file = xppUrl.getFile();
        URL fileUrl = new URL(file);
        proto = fileUrl.getProtocol();
        if (!"file".equals(proto)) {
            throw new FatalException("fileUrl protocol: " + proto);
        }
        file = fileUrl.getFile();
        // allow for "!/"
        String jarPath = file.substring(0, file.length()
                - resourceName.length() - 2);
        JarFile jar = new JarFile(jarPath);
        String name;
        String[] version = null;
        for (Enumeration<JarEntry> e = jar.entries(); e.hasMoreElements();) {
            name = e.nextElement().getName();
            if (name.startsWith(versionPrefix)
                    && name.endsWith(versionSuffix)) {
                name = name.substring(versionPrefix.length(), name
                        .length()
                        - versionSuffix.length());
                logger.info("XPP3 version = " + name);
                version = name.split("\\.");
                break;
            }
        }

        checkXppVersion(version);
    }

    /**
     * @param version
     */
    private void checkXppVersion(String[] version) {
        if (null == version) {
            throw new FatalException(
                    "No version info found - XPP3 is probably too old.");
        }

        // check major, minor, patch for 1+, 1+, and 4+
        int major = Integer.parseInt(version[0]);
        if (major < 1) {
            throw new FatalException(
                    "The XPP3 major version is too old: " + major);
        }
        int minor = Integer.parseInt(version[1]);
        if (1 == major && minor < 1) {
            throw new FatalException(
                    "The XPP3 minor version is too old: " + minor);
        }
        int patch = Integer.parseInt(version[2].replaceFirst(
                "(\\d+)\\D+", "$1"));
        if (1 == major && 1 == minor && patch < 4) {
            throw new FatalException(
                    "The XPP3 patch version is too old: " + version[2]);
        }
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
