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
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
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
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

import com.marklogic.recordloader.Configuration;
import com.marklogic.recordloader.FatalException;
import com.marklogic.recordloader.LoaderException;
import com.marklogic.recordloader.LoaderFactory;
import com.marklogic.recordloader.Monitor;

/**
 * @author Michael Blakeley, <michael.blakeley@marklogic.com>
 * 
 */

public class RecordLoader {

    private static final String SIMPLE_NAME = RecordLoader.class
            .getSimpleName();

    public static final String VERSION = "2008-03-25.1";

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

    private ArrayList<File> xmlFiles = new ArrayList<File>();

    private ArrayList<File> zipFiles = new ArrayList<File>();

    private ArrayList<File> gzFiles = new ArrayList<File>();

    private CharsetDecoder inputDecoder;

    private FileFilter filter;

    private Monitor monitor;

    private LoaderFactory factory;

    private ThreadPoolExecutor pool;

    public RecordLoader(String[] args) throws IOException,
            URISyntaxException {
        configureFiles(Arrays.asList(args).iterator());

        initConfiguration();
        logger.info(printVersion());

        // is the environment healthy?
        checkEnvironment();

        inputDecoder = getDecoder(config.getInputEncoding(), config
                .getMalformedInputAction());

        // handle input-path property, if any
        String path = config.getInputPath();
        if (null != path) {
            logger.info("adding " + path);
            xmlFiles.add(new File(path));
        }

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
            if (arg.endsWith(".properties")) {
                // this will override existing properties
                logger.info("processing: " + arg);
                config.load(new FileInputStream(file));
            } else if (arg.endsWith(".zip")) {
                zipFiles.add(file);
            } else if (arg.endsWith(".gz")) {
                gzFiles.add(file);
            } else {
                xmlFiles.add(file);
            }
        }
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

    private void run() throws FileNotFoundException, IOException,
            LoaderException, SecurityException, NoSuchMethodException,
            ClassNotFoundException {
        logger.fine("zipFiles.size = " + zipFiles.size());
        logger.fine("gzFiles.size = " + gzFiles.size());
        logger.fine("xmlFiles.size = " + xmlFiles.size());

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
        monitor = new Monitor(config, pool);

        try {
            monitor.start();

            if (zipFiles.size() > 0 || gzFiles.size() > 0
                    || xmlFiles.size() > 0) {
                factory = new LoaderFactory(monitor, inputDecoder, config);
                logger.info("populating queue");
                // queue any zip-entries first
                handleZipFiles();
                handleGzFiles();
                handleFiles();
            } else {
                if (config.getThreadCount() > 1) {
                    logger.warning("Will not use multiple threads!");
                    // pointless, since there will only be one input anyway
                    pool.setCorePoolSize(1);
                    pool.setMaximumPoolSize(1);
                }
                // NOTE: cannot use file-based ids
                if (config.isUseFilenameIds()) {
                    logger.warning("Ignoring configured "
                            + Configuration.ID_NAME_KEY + "="
                            + config.getIdNodeName()
                            + " for standard input");
                    config.setUseAutomaticIds();
                }
                factory = new LoaderFactory(monitor, inputDecoder, config);
                handleStandardInput();
            }

            pool.shutdown();

            while (monitor.isAlive()) {
                try {
                    monitor.join();
                } catch (InterruptedException e) {
                    logger.logException("interrupted", e);
                }
            }

            try {
                pool.awaitTermination(60, TimeUnit.SECONDS);
            } catch (InterruptedException e1) {
                e1.printStackTrace();
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
     * @throws IOException
     * 
     */
    private void checkEnvironment() throws IOException {
        // check the XPP3 version
        String resourceName = "META-INF/services/org.xmlpull.v1.XmlPullParserFactory";
        String versionSuffix = "_VERSION";
        String versionPrefix = "XPP3_";

        ClassLoader loader = RecordLoader.class.getClassLoader();
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

    private CharsetDecoder getDecoder(String inputEncoding,
            String malformedInputAction) {
        CharsetDecoder inputDecoder;
        logger.info("using input encoding " + inputEncoding);
        // using an explicit decoder allows us to control the error reporting
        inputDecoder = Charset.forName(inputEncoding).newDecoder();
        if (malformedInputAction
                .equals(Configuration.INPUT_MALFORMED_ACTION_IGNORE)) {
            inputDecoder.onMalformedInput(CodingErrorAction.IGNORE);
        } else if (malformedInputAction
                .equals(Configuration.INPUT_MALFORMED_ACTION_REPLACE)) {
            inputDecoder.onMalformedInput(CodingErrorAction.REPLACE);
        } else {
            inputDecoder.onMalformedInput(CodingErrorAction.REPORT);
        }
        logger.info("using malformed input action "
                + inputDecoder.unmappableCharacterAction().toString());
        inputDecoder.onUnmappableCharacter(CodingErrorAction.REPORT);
        return inputDecoder;
    }

    /**
     * @throws IOException
     * @throws LoaderException
     */
    private void handleFiles() throws IOException, LoaderException {
        filter = new FileFilter() {
            public boolean accept(File _f) {
                return _f.isDirectory()
                        || (_f.isFile() && _f.getName().matches(
                                config.getInputPattern()));
            }
        };

        handleFiles(xmlFiles);
    }

    /**
     * @param _files
     * @throws IOException
     * @throws LoaderException
     */
    private void handleFiles(ArrayList<File> _files) throws IOException,
            LoaderException {
        Iterator<File> iter;
        File file;
        String canonicalPath;
        // queue any files, recursing into directories
        iter = _files.iterator();
        while (iter.hasNext()) {
            file = iter.next();
            canonicalPath = file.getCanonicalPath();
            if (file.isDirectory()) {
                logger.fine("directory " + canonicalPath);
                File[] dirList = file.listFiles(filter);
                if (dirList.length > 0) {
                    logger.info("queuing contents of " + canonicalPath
                            + ": " + dirList.length);
                    ArrayList<File> newlist = new ArrayList<File>();
                    for (int i = 0; i < dirList.length; i++) {
                        newlist.add(dirList[i]);
                    }
                    logger.finer("queuing " + newlist.size() + " items");
                    handleFiles(newlist);
                } else {
                    logger.fine("skipping " + canonicalPath
                            + ": no matches");
                }
                continue;
            }
            logger.fine("queuing " + canonicalPath);
            pool.submit(factory.newLoader(file));
        }
    }

    /**
     * @throws ZipException
     * @throws IOException
     * @throws LoaderException
     */
    private void handleZipFiles() throws ZipException, IOException,
            LoaderException {
        if (null == zipFiles) {
            return;
        }

        String entryName;
        Iterator<File> iter;
        Enumeration<? extends ZipEntry> entries;
        File file;
        ZipFile zipFile;
        ZipEntry ze;
        String zipInputPattern = config.getZipInputPattern();
        String inputPattern = config.getInputPattern();

        // NOTE this technique will intentionally leak zipfile objects!
        iter = zipFiles.iterator();
        int size;
        if (iter.hasNext()) {
            while (iter.hasNext()) {
                file = iter.next();
                zipFile = new ZipFile(file);
                // to avoid closing zipinputstreams randomly,
                // we have to "leak" them temporarily
                // tell the monitor about them, for later cleanup
                monitor.add(zipFile);
                entries = zipFile.entries();
                size = zipFile.size();
                String canonicalPath = file.getCanonicalPath();
                logger.fine("queuing " + size + " entries from zip file "
                        + canonicalPath);
                int count = 0;
                while (entries.hasMoreElements()) {
                    ze = entries.nextElement();
                    logger.fine("found zip entry " + ze);
                    if (ze.isDirectory()) {
                        // skip it
                        continue;
                    }
                    if (!ze.getName().matches(inputPattern)) {
                        // skip it
                        continue;
                    }
                    entryName = ze.getName();
                    if (null != zipInputPattern
                            && !entryName.matches(zipInputPattern)) {
                        // skip it
                        logger.finer("skipping " + entryName);
                        continue;
                    }
                    pool.submit(factory.newLoader(zipFile
                            .getInputStream(ze), file.getName(),
                            entryName));
                    count++;
                    if (0 == count % 1000) {
                        logger.finer("queued " + count
                                + " entries from zip file "
                                + canonicalPath);
                    }
                }
                logger.fine("queued " + count + " entries from zip file "
                        + canonicalPath);
            }
        }
    }

    /**
     * @throws IOException
     * @throws LoaderException
     */
    private void handleGzFiles() throws IOException, LoaderException {
        if (null == gzFiles) {
            return;
        }

        File file;
        String name;
        String path;
        Iterator<File> iter = gzFiles.iterator();

        if (iter.hasNext()) {
            while (iter.hasNext()) {
                file = iter.next();
                name = file.getName();
                if (name.endsWith(".tar.gz") || name.endsWith(".tgz")) {
                    // assume a tar file
                    logger.warning("skipping unsupported tar file "
                            + file.getCanonicalPath());
                    continue;
                }

                path = file.getPath();
                logger.fine("queuing " + path);
                pool.submit(factory.newLoader(new GZIPInputStream(
                        new FileInputStream(file)), name, path));
            }
        }
    }

    private void handleStandardInput() throws LoaderException,
            SecurityException {
        // use standard input
        logger.info("Reading from standard input...");
        pool.submit(factory.newLoader(System.in));
    }

}
