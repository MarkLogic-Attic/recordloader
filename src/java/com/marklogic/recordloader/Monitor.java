/*
 * Copyright (c)2006 Mark Logic Corporation
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.zip.ZipFile;

import com.marklogic.ps.SimpleLogger;
import com.marklogic.ps.timing.TimedEvent;
import com.marklogic.ps.timing.Timer;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class Monitor extends Thread {

    private static SimpleLogger logger;

    private Timer timer;

    private static long lastDisplayMillis = 0;

    String lastUri;

    private boolean running = true;

    private List<ZipFile> openZipFiles;

    private ThreadPoolExecutor pool;

    private Configuration config;

    public void run() {
        try {
            if (logger == null) {
                throw new NullPointerException("must call setLogger");
            }
            logger.fine("starting");
            timer = new Timer();
            monitor();
            logger.info("loaded " + timer.getEventCount()
                    + " records ok (" + timer.getProgressMessage(true)
                    + ")");
        } catch (Exception e) {
            logger.logException("fatal error", e);
        } finally {
            pool.shutdownNow();
            if (openZipFiles != null) {
                logger.fine("cleaning up zip files");
                ZipFile zFile;
                Iterator<ZipFile> iter = openZipFiles.iterator();
                while (iter.hasNext()) {
                    zFile = iter.next();
                    try {
                        zFile.close();
                    } catch (IOException e) {
                        logger.logException("cleaning up "
                                + zFile.getName(), e);
                    }
                }
            }
        }
        logger.fine("exiting");
    }

    /**
     * 
     */
    private void monitor() {
        int displayMillis = Configuration.DISPLAY_MILLIS;
        int sleepMillis = Configuration.SLEEP_TIME;
        long currentMillis;

        // if anything goes wrong, the futuretask knows how to stop us
        logger.finest("looping every " + sleepMillis);
        while (running && !isInterrupted()) {
            currentMillis = System.currentTimeMillis();
            if (lastUri != null
                    && currentMillis - lastDisplayMillis > displayMillis) {
                lastDisplayMillis = currentMillis;
                logger.info("inserted record " + timer.getEventCount()
                        + " as " + lastUri + " ("
                        + timer.getProgressMessage() + ")");
                logger.fine("thread count: core="
                        + pool.getCorePoolSize() + ", active="
                        + pool.getActiveCount());
            }
            try {
                Thread.sleep(sleepMillis);
            } catch (InterruptedException e) {
                logger.logException("sleep was interrupted: continuing",
                        e);
            }

            if (pool.getActiveCount() < 1) {
                logger.fine("no active threads");
                break;
            }
        }
    }

    /**
     * 
     */
    public void halt() {
        logger.info("halting");
        running = false;
        pool.shutdownNow();
        interrupt();
    }

    /**
     * @param _event
     */
    public synchronized void add(String _uri, TimedEvent _event) {
        if (_uri != null) {
            logger.finer("adding event for " + _uri);
            lastUri = _uri;
        }
        timer.add(_event);
    }

    /**
     * @param _logger
     */
    public void setLogger(SimpleLogger _logger) {
        logger = _logger;
    }

    /**
     * @param _pool
     */
    public void setPool(ThreadPoolExecutor _pool) {
        pool = _pool;
    }

    /**
     * @return
     */
    public long getEventCount() {
        return timer.getEventCount();
    }

    /**
     * @param zipFile
     */
    public void add(ZipFile zipFile) {
        if (openZipFiles == null) {
            openZipFiles = new ArrayList<ZipFile>();
        }
        openZipFiles.add(zipFile);
    }

    /**
     * 
     */
    public void resetThreadPool() {
        logger.info("resetting thread pool size");
        int threadCount = config.getThreadCount();
        pool.setMaximumPoolSize(threadCount);
        pool.setCorePoolSize(threadCount);
    }

    /**
     * @param _config
     */
    public void setConfig(Configuration _config) {
        config = _config;
    }

}
