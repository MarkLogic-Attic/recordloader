/**
 * Copyright (c) 2008-2010 Mark Logic Corporation. All rights reserved.
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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.CharsetDecoder;
import java.util.logging.Logger;

import com.marklogic.ps.SimpleLogger;
import com.marklogic.ps.Utilities;
import com.marklogic.ps.timing.TimedEvent;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 *
 */
public abstract class AbstractLoader implements LoaderInterface {

    protected CharsetDecoder decoder;

    protected SimpleLogger logger;

    protected TimedEvent event;

    protected Configuration config;

    protected Monitor monitor;

    protected String currentRecordPath;

    protected String currentFileBasename;

    protected String currentUri;

    protected ContentInterface content;

    protected ContentFactory contentFactory;

    protected String startId;

    protected String entryPath;

    protected String fileBasename;

    protected InputStream input;

    protected File inputFile;

    protected String inputFilePath;

    /**
     * @param _logger
     *
     *            The abstract implementation does nothing. Subclasses may
     *            overload as needed. If problems are encountered, the subclass
     *            should throw a FatalException or another run-time exception.
     */
    public static void checkEnvironment(Logger _logger) {
        // do nothing
    }

    /*
     * (non-Javadoc)
     *
     * @see com.marklogic.recordloader.AbstractLoader#call()
     */
    public Object call() throws Exception {
        try {
            if (null != inputFile) {
                // time to instantiate the reader
                logger.fine("processing " + inputFilePath);
                setInput(new FileInputStream(inputFile), decoder);
            }
            event = new TimedEvent();
            process();
            // preempt the finally block
            cleanup();
            return null;
        } catch (RuntimeException e) {
            // for NullPointerException, etc
            monitor.halt(e);
            return null;
        } catch (Exception e) {
            logger.warning("Exception "
                    + e.toString()
                    + " while processing "
                    + (null != currentUri ? currentUri
                            : currentRecordPath));
            throw e;
        } catch (Throwable t) {
            // for OutOfMemoryError, etc
            monitor.halt(t);
            return null;
        } finally {
            cleanup();
        }
    }

    /**
     * @throws IOException
     */
    private void cleanup() throws IOException {
        // TODO test for entryPath isn't useful, since it's always set
        if (null != fileBasename && null != entryPath) {
            // clean up via monitor
            monitor.cleanup(fileBasename);
            fileBasename = null;
            entryPath = null;
        }
        if (null != input) {
            input.close();
            input = null;
        }
        if (null != inputFile) {
            inputFile = null;
        }
        if (null != contentFactory) {
            contentFactory.close();
            contentFactory = null;
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see com.marklogic.recordloader.LoaderInterface#process()
     *
     * subclasses should override this method to extend it
     */
    @SuppressWarnings("unused")
    public void process() throws LoaderException {
        // safety check
        if (null == input) {
            throw new NullPointerException("caller must set input");
        }

        // cache some info locally
        startId = config.getStartId();
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.marklogic.recordloader.LoaderInterface#setInput(java.io.InputStream)
     */
    @SuppressWarnings("unused")
    public void setInput(InputStream _is, CharsetDecoder _decoder)
            throws LoaderException {
        if (null == _is) {
            throw new NullPointerException("null input stream");
        }
        if (null == _decoder) {
            throw new NullPointerException("null charset decoder");
        }
        input = _is;
        decoder = _decoder;
    }

    /**
     * @param _isError
     * @throws LoaderException
     *
     */
    protected void cleanupInput(boolean _isError) throws LoaderException {
        cleanupRecord();
        if (null == input) {
            return;
        }

        try {
            input.close();
        } catch (IOException e) {
            // nothing we can do about it
            logger.logException(e);
        }

        if (null == inputFile) {
            return;
        }
        if (!config.isDeleteInputFile()) {
            return;
        }
        // if there was a non-fatal error, delete it anyway
        if (_isError && config.isFatalErrors()) {
            return;
        }
        // remove the input file
        try {
            String path = inputFile.getCanonicalPath();
            logger.info("deleting " + path);
            if (!inputFile.delete()) {
                throw new LoaderException("delete failed for " + path);
            }
        } catch (IOException e) {
            throw new LoaderException(e);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see com.marklogic.recordloader.LoaderInterface#setInput(java.io.File)
     */
    public void setInput(File _file, CharsetDecoder _decoder)
            throws LoaderException {
        if (null == _file) {
            throw new NullPointerException("null input file");
        }
        if (null == _decoder) {
            throw new NullPointerException("null charset decoder");
        }
        // defer opening it until call()
        inputFile = _file;
        try {
            inputFilePath = inputFile.getCanonicalPath();
        } catch (IOException e) {
            throw new LoaderException(e);
        }
        decoder = _decoder;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.marklogic.recordloader.LoaderInterface#setFileBasename(java.lang.
     * String)
     */
    public void setFileBasename(String _name) throws LoaderException {
        fileBasename = _name;
        if (null == _name) {
            return;
        }
        currentFileBasename = Utilities.stripExtension(_name);
        logger.fine("using fileBasename = " + _name);

        // don't tell the contentFactory unless config says it's ok
        if (config.isUseFilenameCollection()) {
            contentFactory.setFileBasename(_name);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.marklogic.recordloader.LoaderInterface#setRecordPath(java.lang.String
     * )
     */
    public void setRecordPath(String _path) throws LoaderException {
        entryPath = _path;
        // replace and coalesce any backslashes with slash
        if (config.isInputNormalizePaths()) {
            currentRecordPath = _path.replaceAll("[\\\\]+", "/");
        } else {
            currentRecordPath = _path;
        }

        // this form of URI() does escaping nicely
        if (config.isEscapeUri()) {
            URI uri;
            try {
                // URI(schema, ssp, fragment) constructor cannot handle
                // ssp = 2008-11-07T12:23:47.617766-08:00/1
                // (despite what the javadoc says)...
                // in this situation, treat the path as the fragment.
                uri = new URI(null, currentRecordPath, null);
            } catch (URISyntaxException e) {
                try {
                    uri = new URI(null, null, currentRecordPath);
                } catch (URISyntaxException e1) {
                    throw new LoaderException(e);
                }
            }
            currentRecordPath = uri.toString();
        }

    }

    /**
     * @param len
     *
     */
    protected void updateMonitor(long len) {
        // handle monitor accounting
        // note that we count skipped records, too
        event.increment(len);
        monitor.add(currentUri, event);
    }

    /**
     * @throws LoaderException
     */
    protected void insert() throws LoaderException {
        logger.fine("inserting " + currentUri);
        content.insert();
    }

    /**
     *
     */
    protected void cleanupRecord() {
        // clean up
        logger.fine("content = " + content);
        if (null != content) {
            content.close();
        }
        content = null;
        currentUri = null;
    }

    private boolean checkStartId(String id) {
        if (null == startId) {
            return false;
        }

        // we're still scanning for the startid:
        // is this my cow?
        if (!startId.equals(id)) {
            // don't bother to open the stream: skip this record
            monitor.incrementSkipped("id " + id + " != " + startId);
            return true;
        }

        logger.info("found START_ID " + id);
        startId = null;
        config.setStartId(null);
        // not needed for multithreaded start_id config, but doesn't hurt
        monitor.resetThreadPool();
        return false;
    }

    /**
     * @param _id
     * @return
     * @throws IOException
     * @throws LoaderException
     */
    protected boolean checkIdAndUri(String _id) throws LoaderException,
            IOException {
        return checkStartId(_id) || checkExistingUri(currentUri);
    }

    protected String composeUri(String id) throws IOException {
        logger.finest(id);
        if (null == id) {
            throw new IOException("id may not be null");
        }

        String cleanId = id.trim();

        // TODO move this to the end?
        String inputStripPrefix = config.getInputStripPrefix();
        if (null != inputStripPrefix && inputStripPrefix.length() > 0) {
            cleanId = cleanId.replaceFirst(inputStripPrefix, "");
        }

        if (cleanId.length() < 1) {
            throw new IOException("id may not be empty");
        }

        // automatically use the current file, if available
        // note that config.getUriPrefix() will ensure that the uri ends in '/'
        // TODO differentiate between files and zip archives?
        StringBuilder baseName = new StringBuilder(config.getUriPrefix());

        if (useFileBasename()) {
            baseName.append(currentFileBasename);
        }

        if (null != baseName && baseName.length() > 0
            && ! cleanId.startsWith("/")
            && '/' != baseName.charAt(baseName.length() - 1)) {
            baseName.append("/");
        }
        baseName.append(cleanId);
        baseName.append(config.getUriSuffix());

        String finalName = baseName.toString();
        logger.finest(finalName);
        return finalName;
    }

    /**
     * @return
     */
    private boolean useFileBasename() {
        return null != currentFileBasename
                && !currentFileBasename.equals("")
                && !config.isUseFilenameIds()
                && !config.isIgnoreFileBasename();
    }

    /**
     * @param uri
     * @return
     * @throws IOException
     * @throws LoaderException
     */
    private boolean checkExistingUri(String uri) throws LoaderException,
            IOException {
        // return true if we're supposed to check,
        // and if the document already exists
        if (config.isSkipExisting() || config.isErrorExisting()) {
            boolean exists = content.checkDocumentUri(uri);
            logger.fine("checking for uri " + uri + " = " + exists);
            if (exists) {
                if (config.isErrorExisting()) {
                    throw new LoaderException(
                            "ERROR_EXISTING=true, cannot overwrite existing document: "
                                    + uri);
                }
                // ok, must be skipExisting...
                // count it and log the message
                monitor.incrementSkipped("existing uri " + uri);
                return true;
            } else if (config.isSkipExistingUntilFirstMiss()) {
                synchronized (monitor) {
                    logger.info("resetting "
                            + Configuration.SKIP_EXISTING_KEY + " at "
                            + uri);
                    config.setSkipExisting(false);
                    config.configureThrottling();
                    monitor.resetTimer("skipped");
                }
            }
        }
        return false;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.marklogic.recordloader.LoaderInterface#setConfiguration(com.marklogic
     * .recordloader.Configuration)
     */
    @SuppressWarnings("unused")
    public void setConfiguration(Configuration _config)
            throws LoaderException {
        config = _config;
        logger = config.getLogger();
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.marklogic.recordloader.LoaderInterface#setConnectionUri(java.net.URI)
     */
    public void setConnectionUri(URI _uri) throws LoaderException {
        if (null == config) {
            throw new NullPointerException(
                    "must call setConfiguration() before setUri()");
        }

        // load the correct content factory
        try {
            contentFactory = config.getContentFactoryConstructor()
                    .newInstance(new Object[] {});
        } catch (Exception e) {
            logger.logException(e);
            throw new FatalException(e);
        }
        contentFactory.setConfiguration(config);
        contentFactory.setConnectionUri(_uri);
    }

    /*
     * (non-Javadoc)
     *
     * @seecom.marklogic.recordloader.LoaderInterface#setMonitor(com.marklogic.
     * recordloader.Monitor)
     */
    @SuppressWarnings("unused")
    public void setMonitor(Monitor _monitor) throws LoaderException {
        monitor = _monitor;
    }

}