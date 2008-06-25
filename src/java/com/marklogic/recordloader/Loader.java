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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.MalformedInputException;

import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

import com.marklogic.ps.Utilities;
import com.marklogic.ps.timing.TimedEvent;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */

// Callable<Object> is ok: we really don't return anything
public class Loader extends AbstractLoader {
    private XmlPullParser xpp = null;

    // local cache for hot-loop configuration info
    private String idName;

    private String recordName;

    private String recordNamespace;

    // actual fields

    private Producer producer;

    private ProducerFactory producerFactory;

    private boolean foundRoot = false;

    private boolean useDocumentRoot = false;

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.recordloader.AbstractLoader#process()
     */
    public void process() throws LoaderException {
        super.process();

        logger.fine("auto=" + config.isUseAutomaticIds());
        logger.fine("filename=" + config.isUseFilenameIds());

        try {
            xpp = config.getXppFactory().newPullParser();
            xpp.setInput(new InputStreamReader(input, decoder));
            // TODO feature isn't supported by xpp3 - look at xpp5?
            // xpp.setFeature(XmlPullParser.FEATURE_DETECT_ENCODING, true);
            // TODO feature isn't supported by xpp3 - look at xpp5?
            // xpp.setFeature(XmlPullParser.FEATURE_PROCESS_DOCDECL, true);
            xpp
                    .setFeature(XmlPullParser.FEATURE_PROCESS_NAMESPACES,
                            true);
        } catch (XmlPullParserException e) {
            throw new FatalException(e);
        }

        producerFactory = new ProducerFactory(config, xpp);

        // cache certain info locally
        recordName = config.getRecordName();
        recordNamespace = config.getRecordNamespace();
        useDocumentRoot = config.isUseDocumentRoot();

        try {
            processRecords();
            cleanupInput();
        } catch (Exception e) {
            if (null != inputFile) {
                logger.info("current file: \"" + inputFilePath + "\"");
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

            throw (e instanceof LoaderException) ? (LoaderException) e
                    : new LoaderException(e);
        }
    }

    private void processRecords() {
        int eventType;

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

                    // stop and set error state
                    event.stop(true);
                    monitor.add(currentUri, event);

                    if (content != null) {
                        content.close();
                    }

                    if (config.isUseFilenameIds()) {
                        c = false;
                    }

                    if (e instanceof EOFException) {
                        // there was an error at the end of the file,
                        // so exit the loop
                        c = false;
                    }

                    continue;
                }

                // fatal
                logger.warning("re-throwing fatal error");
                throw new FatalException(e);
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
                throw new FatalException(e);
            }
            logger.logException("non-fatal", e);
        }
    }

    private void processStartElement() throws LoaderException,
            XmlPullParserException, IOException {
        String name = xpp.getName();
        String namespace = xpp.getNamespace();
        logger.finest(name + " in '" + namespace + "'");

        if (!foundRoot) {
            // this must be the document root
            logger.fine("found document root: '" + name + "' in '"
                    + namespace + "'");
            foundRoot = true;
            // if we aren't swallowing the whole doc,
            // then there's nothing more to do here.
            if (!useDocumentRoot) {
                return;
            }
        }

        if (null == recordName) {
            synchronized (config) {
                if (null == config.getRecordName()) {
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

        if (isRecordStart(name, namespace)) {
            // start of a new record
            logger.fine("found record element: '" + recordName + "' in '"
                    + recordNamespace + "'");
            event = new TimedEvent();

            // hand off the work to a new producer
            producer = producerFactory.newProducer();
            String id = producer.getCurrentId();
            logger.fine("found id " + id);

            if (id == null) {
                throw new LoaderException(
                        "producer exited without currentId");
            }

            // must create content object before checking its uri
            currentUri = composeUri(id);
            content = contentFactory.newContent(currentUri);
            producer.setSkippingRecord(checkIdAndUri(id));
            content.setInputStream(producer);

            if (!producer.isSkippingRecord()) {
                insert();
            }

            updateMonitor(producer.getBytesRead());
            cleanupRecord();
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
     * @param name
     * @param namespace
     * @return
     */
    private boolean isRecordStart(String name, String namespace) {
        return useDocumentRoot
                || (name.equals(recordName) && namespace
                        .equals(recordNamespace));
    }

    @Override
    protected void cleanupRecord() {
        super.cleanupRecord();
        producer = null;
    }

    @Override
    public void setConfiguration(Configuration _config)
            throws LoaderException {
        super.setConfiguration(_config);

        // check required configuration
        idName = config.getIdNodeName();
        if (idName == null) {
            throw new FatalException("Missing required property: "
                    + Configuration.ID_NAME_KEY);
        }
    }

}
