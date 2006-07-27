/*
 * Copyright (c)2005-2006 Mark Logic Corporation
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
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

import org.xmlpull.v1.XmlPullParserException;

import com.marklogic.recordloader.Configuration;
import com.marklogic.recordloader.Loader;
import com.marklogic.recordloader.LoaderFactory;
import com.marklogic.recordloader.Monitor;
import com.marklogic.xdbc.XDBCException;

/**
 * @author Michael Blakeley, <michael.blakeley@marklogic.com>
 * 
 */

public class RecordLoader {

    private static final String SIMPLE_NAME = RecordLoader.class
            .getSimpleName();

    public static final String VERSION = "2006-07-27.1";

    public static final String NAME = RecordLoader.class.getName();

    private static SimpleLogger logger = SimpleLogger.getSimpleLogger();

    public static void main(String[] args) throws FileNotFoundException,
            IOException, XDBCException, XmlPullParserException {
        // use system properties as a basis
        // this allows any number of properties at the command-line,
        // using -DPROPNAME=foo
        // as a result, we no longer need any args: default to stdin
        Configuration config = new Configuration();
        List<File> xmlFiles = new ArrayList<File>();
        List<File> zipFiles = new ArrayList<File>();
        Iterator iter = Arrays.asList(args).iterator();
        File file = null;
        String arg = null;
        while (iter.hasNext()) {
            arg = (String) iter.next();
            logger.info("processing argument: " + arg);
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
                config.load(new FileInputStream(file));
            } else if (arg.endsWith(".zip")) {
                // add to zip list
                zipFiles.add(file);
            } else {
                // add to xml list
                xmlFiles.add(file);
            }
        }

        // override with any system props
        config.load(System.getProperties());
        config.setLogger(logger);
        config.configure();

        logger.info(SIMPLE_NAME + " starting, version " + VERSION);

        CharsetDecoder inputDecoder = getDecoder(config
                .getInputEncoding(), config.getMalformedInputAction());

        if (config.getInputPath() != null) {
            // find all the files
            FileFinder ff = new FileFinder(config.getInputPath(), config
                    .getInputPattern());
            ff.find();
            while (ff.size() > 0) {
                file = ff.remove();
                if (file.getName().endsWith(".zip")) {
                    zipFiles.add(file);
                } else {
                    xmlFiles.add(file);
                }
            }
        }

        logger.finer("zipFiles.size = " + zipFiles.size());
        logger.finer("xmlFiles.size = " + xmlFiles.size());

        // if START_ID was supplied, run single-threaded until found
        int threadCount = config.getThreadCount();
        String startId = null;
        if (config.hasStartId()) {
            startId = config.getStartId();
            logger.warning("will single-thread until start-id \""
                    + startId + "\" is reached");
            threadCount = 1;
        }
        ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors
                .newFixedThreadPool(threadCount);

        Monitor monitor = new Monitor();
        monitor.setLogger(logger);
        monitor.setPool(pool);
        monitor.setConfig(config);
        monitor.start();

        if (zipFiles.size() > 0 || xmlFiles.size() > 0) {
            handleFileInput(config, xmlFiles, zipFiles, inputDecoder,
                    monitor, pool);
        } else {
            handleStandardInput(config, inputDecoder, monitor, pool);
        }

        pool.shutdown();

        while (monitor.isAlive()) {
            try {
                monitor.join();
            } catch (InterruptedException e) {
                logger.logException("interrupted", e);
            }
        }
    }

    private static CharsetDecoder getDecoder(String inputEncoding,
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

    private static void handleFileInput(Configuration _config,
            List<File> _xmlFiles, List<File> _zipFiles,
            CharsetDecoder _inputDecoder, Monitor _monitor,
            ThreadPoolExecutor _pool) throws IOException, ZipException,
            FileNotFoundException, XDBCException, XmlPullParserException {

        LoaderFactory factory = new LoaderFactory(logger, _monitor,
                _inputDecoder, _config);
        Loader loader;
        Iterator<File> iter;
        File file;

        logger.info("populating queue");

        // queue any zip-entries first
        // NOTE this technique will intentionally leak zipfile objects!
        iter = _zipFiles.iterator();
        if (iter.hasNext()) {
            ZipFile zipFile;
            Enumeration<? extends ZipEntry> entries;
            ZipEntry ze;
            while (iter.hasNext()) {
                file = iter.next();
                zipFile = new ZipFile(file);
                // to avoid closing zipinputstreams randomly,
                // we have to "leak" them temporarily
                // tell the monitor about them, for later cleanup
                _monitor.add(zipFile);
                entries = zipFile.entries();
                logger.fine("queuing entries from zip file "
                        + file.getCanonicalPath());
                int count = 0;
                while (entries.hasMoreElements()) {
                    ze = entries.nextElement();
                    logger.fine("found zip entry " + ze);
                    if (ze.isDirectory()) {
                        // skip it
                        continue;
                    }
                    loader = factory.newLoader(
                            zipFile.getInputStream(ze), file.getName());
                    submitLoader(_monitor, _pool, loader);
                    count++;
                }
                logger.fine("queued " + count + " entries from zip file "
                        + file.getCanonicalPath());
            }
        }

        // queue any xml files
        iter = _xmlFiles.iterator();
        while (iter.hasNext()) {
            file = iter.next();
            logger.fine("queuing file " + file.getCanonicalPath());
            loader = factory.newLoader(file);
            submitLoader(_monitor, _pool, loader);
        }

        // wait for all threads to complete their work
        logger.info("all files queued");
    }

    private static void handleStandardInput(Configuration _config,
            CharsetDecoder _inputDecoder, Monitor _monitor,
            ThreadPoolExecutor _pool) throws XDBCException,
            XmlPullParserException {
        // use stdin by default
        // NOTE: will not use threads
        logger.info("Reading from standard input...");
        if (_config.getThreadCount() > 1) {
            logger.warning("Will not use multiple threads!");
            _pool.setMaximumPoolSize(1);
        }

        LoaderFactory factory = new LoaderFactory(logger, _monitor,
                _inputDecoder, _config);
        Loader loader = factory.newLoader(System.in, null);
        submitLoader(_monitor, _pool, loader);
    }

    private static Future submitLoader(Monitor _monitor,
            ThreadPoolExecutor pool, Loader loader) {
        Future future = pool.submit(new FutureTask(loader));
        return future;
    }

}
