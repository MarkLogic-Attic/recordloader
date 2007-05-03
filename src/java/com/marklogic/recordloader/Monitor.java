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
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.zip.ZipFile;

import com.marklogic.ps.SimpleLogger;
import com.marklogic.ps.Utilities;
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

    private int totalSkipped = 0;

    private Monitor() {
        // avoid no-argument constructors
    }

    /**
     * @param _c
     * @param _e
     */
    public Monitor(Configuration _c, ThreadPoolExecutor _e) {
        config = _c;
        pool = _e;
        logger = config.getLogger();
    }

    public void run() {
        logger.fine("starting");

        timer = new Timer();
        try {
            monitor();
            // successful exit
            timer.stop();
            logger.info("loaded " + timer.getSuccessfulEventCount()
                    + " records ok (" + timer.getProgressMessage(true)
                    + "), with " + timer.getErrorCount() + " error(s)");
        } catch (Throwable t) {
            logger.logException("fatal error", t);
        } finally {
            cleanup();
        }
        logger.fine("exiting");
        System.exit(0);
    }

    private void cleanup() {
        pool.shutdownNow();

        logger.fine("waiting for pool to terminate");

        try {
            pool.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e1) {
            // do nothing
        }

        if (openZipFiles != null) {
            logger.fine("cleaning up zip files");
            ZipFile zFile;
            Iterator<ZipFile> iter = openZipFiles.iterator();
            while (iter.hasNext()) {
                zFile = iter.next();
                try {
                    zFile.close();
                } catch (IOException e) {
                    logger.logException("cleaning up " + zFile.getName(),
                            e);
                }
            }
        }
    }

    /**
     * 
     */
    private void monitor() throws Exception {
        int displayMillis = Configuration.DISPLAY_MILLIS;
        int sleepMillis = Configuration.SLEEP_TIME;
        long currentMillis;

        // if anything goes wrong, the futuretask knows how to stop us
        // hence, we do nothing with the pool in this loop
        logger.finest("looping every " + sleepMillis);
        while (running && !isInterrupted() && !pool.isTerminated()) {
            // try to avoid thread starvation
            yield();
            
            currentMillis = System.currentTimeMillis();
            if (lastUri != null
                    && currentMillis - lastDisplayMillis > displayMillis) {
                lastDisplayMillis = currentMillis;
                logger.info("inserted record " + timer.getEventCount()
                        + " as " + lastUri + " ("
                        + timer.getProgressMessage() + "), with "
                        + timer.getErrorCount() + " error(s)");
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
        }
    }

    /**
     * 
     */
    public void halt() {
        if (running) {
            logger.info("halting");
            running = false;
            pool.shutdownNow();
            interrupt();
        }
    }

    /**
     * 
     */
    public void halt(Throwable t) {
        logger.logException("fatal - halting monitor", Utilities.getCause(t));
        halt();
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

        // optional throttling
        if (config.isThrottled()) {
            logger.finer("throttling rate " + timer.getEventsPerSecond());
            long sleepMillis;
            while (config.getThrottledEventsPerSecond() < timer
                    .getEventsPerSecond()) {
                sleepMillis = (long) (1000 / config
                        .getThrottledEventsPerSecond() - 1000 / timer
                        .getEventsPerSecond());
                try {
                    Thread.sleep(sleepMillis);
                } catch (InterruptedException e) {
                    logger.logException("interrupted", e);
                }
            }
            logger.fine("throttled rate " + timer.getEventsPerSecond());
        }

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

    /**
     * 
     */
    public void incrementSkipped(String message) {
        logger.log((totalSkipped % 500 == 0) ? Level.INFO
                : Level.FINE, "skipping " + totalSkipped
                + " " + message);        
    }

}
