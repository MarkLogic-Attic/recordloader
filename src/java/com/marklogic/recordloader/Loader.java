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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.nio.charset.MalformedInputException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

import com.marklogic.ps.SimpleLogger;
import com.marklogic.ps.timing.TimedEvent;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.ContentSourceFactory;
import com.marklogic.xcc.DocumentFormat;
import com.marklogic.xcc.Request;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.XccException;
import com.marklogic.xcc.types.XSBoolean;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class Loader implements Callable {
    private SimpleLogger logger;

    private XmlPullParser xpp = null;

    // local cache for hot-loop config info
    private String idName;

    private String startId = null;

    private String recordName;

    private String recordNamespace;

    // actual fields
    private ContentSource conn;

    private Session session;

    private TimedEvent event;

    private ContentCreateOptions docOpts;

    private String currentFileBasename = null;

    private long totalSkipped = 0;

    private Map collectionMap;

    private Configuration config;

    private Monitor monitor;

    private OutputStreamContent currentContent;

    private ProducerThread producer;

    private ProducerThreadFactory factory;

    private boolean foundRoot = false;

    /**
     * @param _monitor
     * @param _uri
     * @param _config
     * @throws XDBCException
     * @throws XmlPullParserException
     */
    public Loader(Monitor _monitor, URI _uri, Configuration _config)
            throws XccException, XmlPullParserException {
        monitor = _monitor;
        config = _config;
        conn = ContentSourceFactory.newContentSource(_uri);
        session = conn.newSession();

        logger = config.getLogger();

        // error if null
        idName = config.getIdNodeName();
        if (idName == null) {
            throw new XmlPullParserException(
                    "Missing required property: "
                            + Configuration.ID_NAME_KEY);
        }
        logger.fine(Configuration.ID_NAME_KEY + "=" + idName);

        // only initialize docOpts once
        if (docOpts == null) {
            boolean resolveEntities = false;
            DocumentFormat format = DocumentFormat.XML;
            int quality = 0;
            docOpts = new ContentCreateOptions();
            docOpts.setResolveEntities(resolveEntities);
            docOpts.setPermissions(config.getPermissions());
            docOpts.setCollections(config.getBaseCollections());
            docOpts.setQuality(quality);
            docOpts.setNamespace(config.getOutputNamespace());
            docOpts.setRepairLevel(config.getRepairLevel());
            docOpts.setPlaceKeys(config.getPlaceKeys());
            docOpts.setFormat(format);
        }

        xpp = config.getXppFactory().newPullParser();
        // TODO feature isn't supported by xpp3 - look at xpp5?
        // xpp.setFeature(XmlPullParser.FEATURE_DETECT_ENCODING, true);
        // TODO feature isn't supported by xpp3 - look at xpp5?
        // xpp.setFeature(XmlPullParser.FEATURE_PROCESS_DOCDECL, true);
        xpp.setFeature(XmlPullParser.FEATURE_PROCESS_NAMESPACES, true);

        factory = new ProducerThreadFactory(this, config);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.concurrent.Callable#call()
     */
    public Object call() throws Exception {
        // cache certain info locally
        startId = config.getStartId();
        recordName = config.getRecordName();
        recordNamespace = config.getRecordNamespace();

        try {
            process();
            return null;
        } catch (Exception e) {
            if (currentContent != null) {
                logger.info("current uri: \"" + currentContent.getUri()
                        + "\"");
                if (producer != null) {
                    logger.info("current record:\n" + producer);
                }
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
            logger.logException("fatal - halting monitor", e);
            monitor.halt();
            throw e;
        }
    }

    /**
     * @param _reader
     * @throws XmlPullParserException
     */
    public void setInput(Reader _reader) throws XmlPullParserException {
        xpp.setInput(_reader);
    }

    /**
     * @param _file
     * @throws XmlPullParserException
     * @throws FileNotFoundException
     */
    public void setInput(File _file) throws FileNotFoundException,
            XmlPullParserException {
        setInput(new FileReader(_file));
    }

    /**
     * @param _path
     */
    public void setFileBasename(String _name) {
        currentFileBasename = _name;
        // update collections
        if (currentFileBasename == null) {
            docOpts.setCollections(config.getBaseCollections());
        } else {
            List<String> newCollections = new ArrayList<String>(Arrays
                    .asList(config.getBaseCollections()));
            newCollections.add(_name);
            docOpts.setCollections(newCollections.toArray(new String[0]));
        }
        logger.info("using fileBasename = " + currentFileBasename);
    }

    public void process() throws Exception {
        int eventType;

        // NOTE: next() skips comments, document-decl, ignorable-whitespace,
        // processing-instructions automatically.
        // to catch these, use nextToken() instead.
        // nextToken() could also be used for custom entity handling.
        boolean c = true;
        while (c) {
            // We *only* care about finding records,
            // then passing them off a new producer
            try {
                eventType = xpp.next();
                switch (eventType) {
                case XmlPullParser.START_TAG:
                    processStartElement();
                    break;
                case XmlPullParser.TEXT:
                    // text in an unknown element, or otherwise a no-op
                    break;
                case XmlPullParser.END_TAG:
                    // end of an unknown element, or otherwise a no-op
                    break;
                case XmlPullParser.START_DOCUMENT:
                    break;
                case XmlPullParser.END_DOCUMENT:
                    c = false;
                    break;
                default:
                    throw new XmlPullParserException("UNIMPLEMENTED: "
                            + eventType);
                }
            } catch (Exception e) {
                if (!config.isFatalErrors()) {
                    // keep going
                    logger.logException("non-fatal: skipping", e);

                    event.stop(true);
                    monitor.add(currentContent.getUri(), event);

                    // closeCurrentRecord();
                    continue;
                }

                // fatal
                throw e;
            }
        }

        if (currentContent != null || producer != null) {
            throw new XmlPullParserException(
                    "end of document before end of current record!\n"
                            + "recordName = " + recordName
                            + ", recordNamespace = " + recordNamespace
                            + " at " + xpp.getPositionDescription()
                            + "\n" + currentContent.getUri());
        }
    }

    private void processStartElement() throws Exception {
        String name = xpp.getName();
        String namespace = xpp.getNamespace();
        logger.finest(name + " in '" + namespace + "'");

        // TODO preserve default namespace and prefix declarations
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

            // hand it off to a new producer thread
            producer = factory.newProducerThread();
            producer.setLoaderThread(Thread.currentThread());
            producer.start();
            while (producer.isAlive()) {
                try {
                    producer.join();
                } catch (InterruptedException e) {
                    logger.finer("interrupted");
                    if (producer.isAlive()) {
                        String id = producer.getCurrentId();
                        if (id != null) {
                            logger.fine("found id " + id);
                            composeDocOptions(id);
                            String uri = composeUri(id);
                            currentContent = new OutputStreamContent(uri,
                                    docOpts);

                            producer.setOutputStream(currentContent
                                    .getOutputStream());
                            // start inserting now, so we don't block
                            session.insertContent(currentContent);
                        }
                    } else {
                        logger.logException("interrupted", e);
                    }
                }
            }

            // check for exceptions
            Exception e = producer.getException();
            if (e != null) {
                throw e;
            }

            if (producer.getCurrentId() == null) {
                throw new XmlPullParserException(
                        "producer returned without finding an id");
            }
            
            if (currentContent == null) {
                throw new IOException("unexpected null currentContent");
            }

            // finish content insertion
            session.commit();

            logger.fine("commit ok for " + currentContent.getUri());

            // handle monitor accounting
            event.increment(producer.getBytesWritten());
            monitor.add(currentContent.getUri(), event);

            // clean up
            currentContent.close();
            producer = null;
            currentContent = null;

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

    private void composeDocOptions(String _id) {
        // docOptions have already been initialized,
        // but may need more work:
        // handle collectionsMap, if present
        if (collectionMap == null) {
            return;
        }

        // in this case we have to reset the whole collection list every
        // time, to prevent any carryover from the previous call to
        // docOptions.setCollections().
        List<String> collections = new ArrayList<String>(Arrays
                .asList(config.getBaseCollections()));
        if (currentFileBasename != null) {
            collections.add(currentFileBasename);
        }
        if (collectionMap.containsKey(_id)) {
            // each map entry is a String[]
            collections.addAll(Arrays.asList((String[]) collectionMap
                    .get(_id)));
        }
        docOpts.setCollections(collections.toArray(new String[0]));
    }

    /**
     * @param _uri
     * @return
     */
    private boolean checkFile(String _uri) throws XccException {
        // TODO this is a common pattern: should be in a reusable class
        String query = "define variable $URI as xs:string external\n"
                + "xdmp:exists(doc($URI))\n";
        ResultSequence result = null;
        boolean exists = false;
        try {
            Request request = session.newAdhocQuery(query);
            request.setNewStringVariable("URI", _uri);

            result = session.submitRequest(request);

            if (!result.hasNext()) {
                throw new RequestException("unexpected null result",
                        request);
            }

            ResultItem item = result.next();

            exists = ((XSBoolean) item.getItem()).asPrimitiveBoolean();
        } finally {
            if (result != null && !result.isClosed())
                result.close();
        }
        return exists;
    }

    /**
     * @param uri
     * @return
     * @throws IOException
     * @throws XDBCException
     */
    private boolean checkExistingUri(String uri) throws XccException,
            IOException {
        // return true if we're supposed to check,
        // and if the document already exists
        if (config.isSkipExisting() || config.isErrorExisting()) {
            boolean exists = checkFile(uri);
            logger.fine("checking for uri " + uri + " = " + exists);
            if (exists) {
                if (config.isErrorExisting()) {
                    throw new IOException(
                            "ERROR_EXISTING=true, cannot overwrite existing document: "
                                    + uri);
                }
                // ok, must be skipExisting...
                logger.info("skipping " + (++totalSkipped)
                        + " existing uri " + uri);
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
        return config.getUriPrefix()
                + ((currentFileBasename == null || currentFileBasename
                        .equals("")) ? "" : currentFileBasename + "/")
                + cleanId + config.getUriSuffix();
    }

    // private void closeCurrentRecord() throws IOException {
    // // if (current != null) {
    // // current.close();
    // // }
    // if (currentContent != null) {
    // currentContent.close();
    // }
    // currentContent = null;
    // // current = null;
    // event = null;
    // //skippingRecord = false;
    // }

    /**
     * @param _map
     */
    public void setCollectionMap(Map _map) {
        collectionMap = _map;
    }

    /**
     * @return
     */
    public XmlPullParser getParser() {
        return xpp;
    }

    private boolean checkStartId(String id) {
        if (startId == null)
            return false;

        // we're still scanning for the startid:
        // is this my cow?
        if (!startId.equals(id)) {
            // don't bother to open the stream: skip this record
            logger.info("skipping record " + (++totalSkipped)
                    + " with id " + id + " != " + startId);
            return true;
        }

        logger.info("found START_ID " + id);
        startId = null;
        config.setStartId(null);
        monitor.resetThreadPool();
        return false;
    }

    /**
     * @param id
     * @return
     * @throws IOException
     * @throws XccException
     */
    public boolean checkId(String id) throws XccException, IOException {
        if (checkStartId(id)) {
            return true;
        }

        String uri = composeUri(id);
        if (checkExistingUri(uri)) {
            return true;
        }

        return false;
    }

}
