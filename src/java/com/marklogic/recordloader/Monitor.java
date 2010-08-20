/*
 * Copyright (c)2006-2009 Mark Logic Corporation
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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

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

    private volatile Timer timer;

    private static long lastDisplayMillis = 0;

    private volatile String lastUri;

    private boolean running = true;

    protected Map<String, ZipReference> openZipFiles = Collections
            .synchronizedMap(new HashMap<String, ZipReference>());

    private ThreadPoolExecutor pool;

    private Configuration config;

    private int totalSkipped = 0;

    private Thread parent;

    private int lastSkipped = 0;

    private long lastCount = 0;

    @SuppressWarnings("unused")
    private Monitor() {
        // avoid no-argument constructors
    }

    /**
     * @param _c
     * @param _p
     */
    public Monitor(Configuration _c, Thread _p) {
        config = _c;
        parent = _p;
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
    }

    private void cleanup() {
        pool.shutdownNow();

        logger.fine("waiting for pool to terminate");

        try {
            pool.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e1) {
            // interrupt status will be reset below,
            // in case parent re-interrupts us
        }

        // NB - we used to call System.exit(0) here - necessary?
        // sometimes the main RecordLoader thread will hit CallerBlocksPolicy
        parent.interrupt();

        if (isInterrupted()) {
            logger.info("resetting interrupt status");
            interrupted();
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
        while (running && !isInterrupted()) {
            // try to avoid thread starvation
            yield();

            currentMillis = System.currentTimeMillis();
            if (lastUri != null
                    && currentMillis - lastDisplayMillis > displayMillis
                    && (lastSkipped < totalSkipped || lastCount < timer
                            .getEventCount())) {
                lastDisplayMillis = currentMillis;
                lastSkipped = totalSkipped;
                // events include errors
                lastCount = timer.getEventCount();
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
                // interrupt status will be reset below
            }
        }
        if (isInterrupted()) {
            interrupted();
        }
    }

    /**
     * 
     */
    public void halt() {
        if (!running) {
            return;
        }
        logger.info("halting");
        running = false;
        pool.shutdownNow();
        // for quicker shutdown
        interrupt();
    }

    /**
     * 
     */
    public void halt(Throwable t) {
        logger.warning("fatal - halting monitor");
        logger.logException(t.getMessage(), Utilities.getCause(t));
        halt();
    }

    /**
     * @param _uri
     * @param _event
     */
    public synchronized void add(String _uri, TimedEvent _event) {
        if (_uri != null) {
            logger.finer("adding event for " + _uri);
            lastUri = _uri;
        }
        // do not keep the TimedEvent objects in the timer: 48-B each
        timer.add(_event, false);

        checkThrottle();

    }

    /**
     * 
     */
    private void checkThrottle() {
        // optional throttling
        if (!config.isThrottled()) {
            return;
        }
        long sleepMillis;
        double throttledEventsPerSecond = config
                .getThrottledEventsPerSecond();
        boolean isEvents = (throttledEventsPerSecond > 0);
        int throttledBytesPerSecond = isEvents ? 0 : config
                .getThrottledBytesPerSecond();
        logger.fine("throttling "
                + (isEvents
                // events
                ? (timer.getEventsPerSecond() + " tps to "
                        + throttledEventsPerSecond + " tps")
                        // bytes
                        : (timer.getBytesPerSecond() + " B/sec to "
                                + throttledBytesPerSecond + " B/sec")));
        // call the methods every time
        while ((throttledEventsPerSecond > 0 && (throttledEventsPerSecond < timer
                .getEventsPerSecond()))
                || (throttledBytesPerSecond > 0 && (throttledBytesPerSecond < timer
                        .getBytesPerSecond()))) {
            if (isEvents) {
                sleepMillis = (long) Math
                        .ceil(Timer.MILLISECONDS_PER_SECOND
                                * ((timer.getEventCount() / throttledEventsPerSecond) - timer
                                        .getDurationSeconds()));
            } else {
                sleepMillis = (long) Math
                        .ceil(Timer.MILLISECONDS_PER_SECOND
                                * ((timer.getBytes() / throttledBytesPerSecond) - timer
                                        .getDurationSeconds()));
            }
            sleepMillis = Math.max(sleepMillis, 1);
            logger.finer("sleeping " + sleepMillis);
            try {
                Thread.sleep(sleepMillis);
            } catch (InterruptedException e) {
                // caller will reset interrupted status
            }
        }
        logger.fine("throttled to "
                + (isEvents ? (timer.getEventsPerSecond() + " tps")
                        : (timer.getBytesPerSecond() + " B/sec")));
    }

    /**
     * @param fileName
     */
    protected void cleanup(String fileName) {
        // clean up any zip references
        ZipReference ref = openZipFiles.get(fileName);
        if (null == ref) {
            // TODO must ignore for now
            // throw new NullPointerException("no reference to " + fileName);
            return;
        }
        ref.closeReference();
    }

    /**
     * @return
     */
    public long getEventCount() {
        return timer.getEventCount();
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
        totalSkipped++;
        logger.log((totalSkipped % 500 == 0) ? Level.INFO : Level.FINE,
                "skipping " + totalSkipped + ": " + message);
    }

    public ThreadPoolExecutor getPool() {
        return pool;
    }

    public void setPool(ThreadPoolExecutor pool) {
        this.pool = pool;
    }

    /**
     * @param zipFile
     * @param zipFileName
     */
    public void add(ZipReference zipFile, String zipFileName) {
        // queue for later cleanup
        openZipFiles.put(zipFileName, zipFile);
    }

    /**
     * @param _msg
     */
    public synchronized void resetTimer(String _msg) {
        timer.stop();
        logger.info(_msg + " " + timer.getSuccessfulEventCount()
                + " records ok (" + timer.getProgressMessage(true)
                + "), with " + timer.getErrorCount() + " error(s)");
        timer = new Timer();
    }

    public void instanceInterrupted() {
        interrupted();
    }

}
