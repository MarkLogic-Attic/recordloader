/*
 * Copyright (c)2006-2008 Mark Logic Corporation
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
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.MalformedInputException;
import java.util.concurrent.Callable;

import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

import com.marklogic.ps.SimpleLogger;
import com.marklogic.ps.Utilities;
import com.marklogic.ps.timing.TimedEvent;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */

// Callable<Object> is ok: we really don't return anything
public class Loader implements Callable<Object> {
    private SimpleLogger logger;

    private XmlPullParser xpp = null;

    // local cache for hot-loop configuration info
    private String idName;

    private String startId = null;

    private String recordName;

    private String recordNamespace;

    // actual fields

    private TimedEvent event;

    private String currentFileBasename = null;

    private Configuration config;

    private Monitor monitor;

    private Producer producer;

    private ProducerFactory producerFactory;

    private boolean foundRoot = false;

    private File inputFile;

    private String currentRecordPath;

    private String currentUri;

    private Reader input;

    private ContentInterface content;

    private ContentFactory contentFactory;

    /**
     * @param _monitor
     * @param _uri
     * @param _config
     * @throws LoaderException
     */
    public Loader(Monitor _monitor, URI _uri, Configuration _config)
            throws LoaderException {
        monitor = _monitor;
        config = _config;

        // error if null
        idName = config.getIdNodeName();
        if (idName == null) {
            throw new FatalException("Missing required property: "
                    + Configuration.ID_NAME_KEY);
        }

        logger = config.getLogger();

        // try to load the correct content factory
        try {
            contentFactory = config.getContentFactoryConstructor()
                    .newInstance(new Object[] {});
        } catch (Exception e) {
            logger.logException(e);
            throw new FatalException(e);
        }
        contentFactory.setConnectionUri(_uri);
        contentFactory.setConfiguration(config);

    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.concurrent.Callable#call()
     * 
     * NB - always returns null
     */
    public Object call() throws Exception {

        logger.fine(Configuration.ID_NAME_KEY + "=" + idName);

        initParser();
        // TODO feature isn't supported by xpp3 - look at xpp5?
        // xpp.setFeature(XmlPullParser.FEATURE_DETECT_ENCODING, true);
        // TODO feature isn't supported by xpp3 - look at xpp5?
        // xpp.setFeature(XmlPullParser.FEATURE_PROCESS_DOCDECL, true);
        xpp.setFeature(XmlPullParser.FEATURE_PROCESS_NAMESPACES, true);

        producerFactory = new ProducerFactory(config, xpp);

        // cache certain info locally
        startId = config.getStartId();
        recordName = config.getRecordName();
        recordNamespace = config.getRecordNamespace();

        if (config.isUseFileNameIds()) {
            // every file is its own root record
            logger.finer("treating input as a record");
            foundRoot = true;
        }

        FileReader fileReader = null;
        try {
            if (inputFile != null) {
                // time to instantiate the reader
                fileReader = new FileReader(inputFile);
                setInput(fileReader);
            }
            process();
            // don't rely on the finally block too much
            contentFactory.close();
            contentFactory = null;
            return null;
        } catch (Exception e) {
            if (null != inputFile) {
                logger.info("current file: \""
                        + inputFile.getCanonicalPath() + "\"");
            }
            if (null != currentFileBasename) {
                logger.info("current file basename: \""
                        + currentFileBasename + "\"");
            }
            logger.info("current uri: \"" + currentUri + "\"");
            if (producer != null) {
                logger.info("current record: " + producer + ", bytes = "
                        + producer.getByteBufferDescription());
            }
            if (e instanceof MalformedInputException) {
                // invalid character sequence, probably
                logger.warning("input could not be decoded: try setting "
                        + Configuration.INPUT_ENCODING_KEY + " (or set "
                        + Configuration.INPUT_MALFORMED_ACTION_KEY
                        + " to "
                        + Configuration.INPUT_MALFORMED_ACTION_IGNORE
                        + " or "
                        + Configuration.INPUT_MALFORMED_ACTION_REPLACE
                        + ").");
            }

            // if it got this high, it's fatal
            monitor.halt(e);
            throw e;
        } catch (Throwable t) {
            // for OutOfMemoryError, NullPointerException, etc
            monitor.halt(t);
            return null;
        } finally {
            // if we have a file reader open, close it
            if (fileReader != null) {
                fileReader.close();
            }
            if (null != contentFactory) {
                contentFactory.close();
            }
        }
    }

    /**
     * @param _reader
     * @throws XmlPullParserException
     */
    public void setInput(Reader _reader) throws XmlPullParserException {
        if (null == _reader) {
            throw new NullPointerException("null reader");
        }
        initParser();
        xpp.setInput(_reader);
        input = _reader;
    }

    private void initParser() throws XmlPullParserException {
        if (null != xpp) {
            return;
        }
        xpp = config.getXppFactory().newPullParser();
    }

    /**
     * @param _file
     */
    public void setInput(File _file) {
        // defer until we actually need to open it
        inputFile = _file;
    }

    /**
     * @param _path
     */
    public void setFileBasename(String _name) throws LoaderException {
        logger.fine("using fileBasename = " + _name);
        if (null == _name) {
            return;
        }
        currentFileBasename = _name;
        contentFactory.setFileBasename(_name);
    }

    private void process() throws Exception {
        int eventType;

        // TODO sometimes we can short-circuit the parsing
        // for example, if the filename is the id.
        // TODO do we need to check anything else for this?
        if (config.isUseFileNameIds()) {
            processMonolith();
            return;
        }

        boolean c = true;
        while (c) {
            try {
                eventType = xpp.nextToken();
                switch (eventType) {
                // We *only* care about finding records,
                // then passing them off a new producer
                case XmlPullParser.START_TAG:
                    processStartElement();
                    break;
                case XmlPullParser.END_DOCUMENT:
                    c = false;
                    break;
                default:
                    break;
                }
            } catch (Exception e) {
                if (currentFileBasename != null) {
                    logger.warning("error in "
                            + currentFileBasename
                            + (currentRecordPath == null ? ""
                                    : (" at " + currentRecordPath)));
                }
                if (null != currentUri) {
                    logger.warning("current URI = " + currentUri);
                }
                if (producer != null) {
                    logger.warning(producer.getByteBufferDescription());
                    logger.warning("buffer = " + producer.getBuffer());
                }
                if (xpp != null) {
                    logger.warning("pos = "
                            + xpp.getPositionDescription());
                    logger.warning("text = " + xpp.getText());
                }
                // get to the init cause, if there is one
                logger.logException("exception", Utilities.getCause(e));
                if (!config.isFatalErrors()) {
                    // keep going
                    logger.logException("non-fatal: skipping", e);

                    event.stop(true);
                    monitor.add(currentUri, event);

                    if (content != null) {
                        content.close();
                    }

                    if (config.isUseFileNameIds()) {
                        c = false;
                    }
                    continue;
                }

                // fatal
                logger.warning("re-throwing fatal error");
                throw e;
            }
        }

        if (null != content) {
            XmlPullParserException e = new XmlPullParserException(
                    "end of document before end of current record!\n"
                            + "recordName = " + recordName
                            + ", recordNamespace = " + recordNamespace
                            + " at " + xpp.getPositionDescription()
                            + "\n" + currentUri);
            if (config.isFatalErrors()) {
                throw e;
            }
            logger.logException("non-fatal", e);
            cleanup();
        }
    }

    /**
     * @throws URISyntaxException
     * @throws IOException
     * @throws LoaderException
     * 
     */
    private void processMonolith() throws URISyntaxException,
            LoaderException, IOException {
        try {
            // handle the input reader as a single document,
            // without any parsing.

            boolean useFileNameIds = config.isUseFileNameIds();
            logger.fine("isUseFileNameIds=" + useFileNameIds);
            if (!useFileNameIds) {
                throw new FatalException("wrong code path");
            }

            event = new TimedEvent();

            String id = currentRecordPath;

            // Regex replaces and coalesces any backslashes with slash
            if (config.isInputNormalizePaths()) {
                id = currentRecordPath.replaceAll("[\\\\]+", "/");
            }

            String inputStripPrefix = config.getInputStripPrefix();
            if (inputStripPrefix != null && inputStripPrefix.length() > 0) {
                id = id.replaceFirst(inputStripPrefix, "");
            }

            // this form of URI() does escaping nicely
            id = new URI(null, id, null).toString();

            logger.fine("setting currentId = " + id);

            // we need the content object, hence the URI, before we can check
            // its existence
            currentUri = composeUri(id);
            content = contentFactory.newContent(currentUri);
            boolean skippingRecord = checkId(id);

            // grab the entire document
            // uses a reader, so charset translation should be ok
            StringBuffer sb = new StringBuffer();
            int size;
            char[] buf = new char[32 * 1024];
            while ((size = input.read(buf)) > 0) {
                sb.append(buf, 0, size);
            }
            String xml = sb.toString();

            if (!skippingRecord) {
                logger.fine("inserting " + currentUri);
                content.setXml(xml);
                content.insert();
            }

            // handle monitor accounting
            // note that we count skipped records, too
            event.increment(xml.length());
            monitor.add(currentUri, event);
        } catch (URISyntaxException e) {
            if (config.isFatalErrors()) {
                throw e;
            }
        } catch (LoaderException e) {
            if (config.isFatalErrors()) {
                throw e;
            }
        } catch (IOException e) {
            if (config.isFatalErrors()) {
                throw e;
            }
        } finally {
            cleanup();
        }
    }

    /**
     * 
     */
    private void cleanup() {
        // clean up
        if (null != content) {
            content.close();
        }
        producer = null;
        content = null;
        currentUri = null;
    }

    private void processStartElement() throws LoaderException,
            URISyntaxException, XmlPullParserException, IOException {
        String name = xpp.getName();
        String namespace = xpp.getNamespace();
        logger.finest(name + " in '" + namespace + "'");

        if (!foundRoot) {
            // this must be the document root
            logger.fine("found document root: '" + name + "' in '"
                    + namespace + "'");
            foundRoot = true;
            return;
        }

        if (recordName == null) {
            synchronized (config) {
                if (config.getRecordName() == null) {
                    // this must be the record-level element
                    recordName = name;
                    recordNamespace = namespace;
                    config.setRecordName(recordName);
                    config.setRecordNamespace(namespace);
                    logger.fine("autodetected record element: '"
                            + recordName + "' in '" + recordNamespace
                            + "'");
                } else {
                    // another thread beat us to it
                    recordName = config.getRecordName();
                    recordNamespace = config.getRecordNamespace();
                }
            }
        }

        if (name.equals(recordName) && namespace.equals(recordNamespace)) {
            // start of a new record
            logger.fine("found record element: '" + recordName + "' in '"
                    + recordNamespace + "'");
            event = new TimedEvent();

            // hand off the work to a new producer
            producer = producerFactory.newProducer();
            String id = null;
            if (config.isUseFileNameIds()) {
                // this form of URI() does escaping nicely
                id = new URI(null, currentRecordPath, null).toString();
                logger.fine("setting currentId = " + id);
                producer.setCurrentId(id);
            } else {
                id = producer.getCurrentId();
                logger.fine("found id " + id);
            }

            if (id == null) {
                throw new LoaderException(
                        "producer exited without currentId");
            }

            // must create content object before checking its uri
            currentUri = composeUri(id);
            content = contentFactory.newContent(currentUri);
            producer.setSkippingRecord(checkId(id));
            content.setProducer(producer);

            if (!producer.isSkippingRecord()) {
                logger.fine("inserting " + currentUri);
                content.insert();
            }

            // handle monitor accounting
            // note that we count skipped records, too
            event.increment(producer.getBytesRead());
            monitor.add(currentUri, event);

            cleanup();
            return;
        }

        // handle unknown element
        if (config.isIgnoreUnknown()) {
            logger
                    .warning("skipping unknown non-record element: "
                            + name);
            return;
        }
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
                    throw new IOException(
                            "ERROR_EXISTING=true, cannot overwrite existing document: "
                                    + uri);
                }
                // ok, must be skipExisting...
                // count it and log the message
                monitor.incrementSkipped("existing uri " + uri);
                return true;
            }
        }
        return false;
    }

    private String composeUri(String id) throws IOException {
        if (id == null) {
            throw new IOException("id may not be null");
        }

        String cleanId = id.trim();
        if (cleanId.length() < 1) {
            throw new IOException("id may not be empty");
        }

        // automatically use the current file, if available
        // note that config.getUriPrefix() will ensure that the uri ends in '/'
        StringBuffer baseName = new StringBuffer(config.getUriPrefix());
        baseName.append((currentFileBasename == null
                || currentFileBasename.equals("") || config
                .isUseFileNameIds()) ? "" : currentFileBasename);
        if (baseName != null && baseName.length() > 0
                && '/' != baseName.charAt(baseName.length() - 1)) {
            baseName.append("/");
        }
        baseName.append(cleanId);
        baseName.append(config.getUriSuffix());
        return baseName.toString();
    }

    private boolean checkStartId(String id) {
        if (startId == null) {
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
        monitor.resetThreadPool();
        return false;
    }

    /**
     * @param _id
     * @return
     * @throws IOException
     * @throws LoaderException
     */
    private boolean checkId(String _id) throws LoaderException,
            IOException {
        return checkStartId(_id) || checkExistingUri(composeUri(_id));
    }

    /**
     * @param _path
     */
    public void setRecordPath(String _path) {
        currentRecordPath = _path;
    }

}
