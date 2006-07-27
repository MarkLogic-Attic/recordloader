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
import java.nio.charset.MalformedInputException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;

import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

import com.marklogic.ps.Connection;
import com.marklogic.ps.SimpleLogger;
import com.marklogic.ps.Utilities;
import com.marklogic.ps.timing.TimedEvent;
import com.marklogic.xdbc.XDBCException;
import com.marklogic.xdbc.XDBCXQueryException;
import com.marklogic.xdmp.XDMPDocInsertStream;
import com.marklogic.xdmp.XDMPDocOptions;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class Loader implements Callable {
    private static SimpleLogger logger;

    private XmlPullParser xpp = null;

    private String rootName;

    private String rootNamespace;

    // local cache for hot-loop config info
    private String idName;

    private String recordName;

    private String recordNamespace;

    private String startId = null;

    // actual fields
    private Connection conn;

    private boolean skippingRecord = false;

    private TimedEvent event;

    private XDMPDocOptions docOpts;

    private OutputDocument current = null;

    private String currentFileBasename = null;

    private long totalSkipped = 0;

    private Map collectionMap;

    private Configuration config;

    private Monitor monitor;

    /**
     * @param _monitor
     * @param _connectionString
     * @param _config
     * @throws XDBCException
     * @throws XmlPullParserException
     */
    public Loader(Monitor _monitor, String _connectionString,
            Configuration _config) throws XDBCException,
            XmlPullParserException {
        monitor = _monitor;
        config = _config;
        conn = new Connection(_connectionString);

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
            int format = XDMPDocInsertStream.XDMP_DOC_FORMAT_XML;
            int quality = 0;
            String language = null;
            docOpts = new XDMPDocOptions(Locale.getDefault(),
                    resolveEntities, config.getPermissions(), config
                            .getBaseCollections(), quality, config
                            .getOutputNamespace(), config
                            .getRepairLevel(), config.getPlaceKeys(),
                    format, language);
        }

        xpp = config.getXppFactory().newPullParser();
        // TODO feature isn't supported by xpp3 - look at xpp5?
        // xpp.setFeature(XmlPullParser.FEATURE_DETECT_ENCODING, true);
        // TODO feature isn't supported by xpp3 - look at xpp5?
        // xpp.setFeature(XmlPullParser.FEATURE_PROCESS_DOCDECL, true);
        xpp.setFeature(XmlPullParser.FEATURE_PROCESS_NAMESPACES, true);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.concurrent.Callable#call()
     */
    public Object call() throws Exception {
        // cache certain info locally
        recordName = config.getRecordName();
        recordNamespace = config.getRecordNamespace();
        startId = config.getStartId();

        try {
            process();
            return null;
        } catch (Exception e) {
            if (current != null) {
                logger.info("current uri: \"" + current.getUri() + "\"");
                logger.info("current characters: \""
                        + getCurrentTextCharactersString() + "\"");
                if (current != null) {
                    logger.info("current record:\n" + current);
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

    public void process() throws XmlPullParserException, IOException,
            XDBCException {
        int eventType;

        // NOTE: next() skips comments, document-decl, ignorable-whitespace,
        // processing-instructions automatically.
        // to catch these, use nextToken() instead.
        // nextToken() could also be used for custom entity handling.
        boolean c = true;
        while (c) {
            try {
                eventType = xpp.next();
                switch (eventType) {
                case XmlPullParser.START_TAG:
                    processStartElement();
                    break;
                case XmlPullParser.TEXT:
                    processText();
                    break;
                case XmlPullParser.END_TAG:
                    processEndElement();
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
            } catch (XmlPullParserException e) {
                logger.warning(e.getClass().getSimpleName() + " in "
                        + currentFileBasename + " at "
                        + xpp.getPositionDescription());
                // could be a problem entity
                if (e.getMessage().contains("entity")) {
                    logger.warning("entity error: " + e.getMessage());
                    handleUnresolvedEntity();
                } else if (e.getMessage().contains(
                        "quotation or apostrophe")
                        && config.isFullRepair()) {
                    // messed-up attribute? skip it?
                    logger.warning("attribute error: " + e.getMessage());
                    // all we can do is ignore it, apparently
                } else {
                    throw e;
                }
            } catch (XDBCException e) {
                if (!config.isFatalErrors()) {
                    // keep going
                    logger.logException("non-fatal: skipping", e);

                    event.stop(true);
                    monitor.add(current.getUri(), event);

                    closeCurrentRecord();
                    continue;
                }

                // fatal
                throw e;
            }
        }

        if (current != null) {
            throw new XmlPullParserException(
                    "end of document before end of current record!\n"
                            + "recordName = " + recordName
                            + ", recordNamespace = " + recordNamespace
                            + " at " + xpp.getPositionDescription()
                            + "\n" + current.getUri());
        }
    }

    /**
     * @throws IOException
     * @throws XmlPullParserException
     * @throws XDBCException
     * 
     */
    private void handleUnresolvedEntity() throws XmlPullParserException,
            IOException, XDBCException {
        int type;
        boolean c = true;
        while (c) {
            try {
                type = xpp.nextToken();
            } catch (XmlPullParserException e) {
                if (e.getMessage().contains("quotation or apostrophe")
                        && config.isFullRepair()) {
                    // messed-up attribute? skip it?
                    logger.warning("attribute error: " + e.getMessage());
                    // all we can do is ignore it, apparently
                    return;
                }
                throw e;
            }
            logger.fine("type=" + type);
            switch (type) {
            case XmlPullParser.TEXT:
                processText();
                break;
            case XmlPullParser.ENTITY_REF:
                processMalformedEntityRef();
                break;
            case XmlPullParser.START_TAG:
                processStartElement();
                return;
            // break;
            case XmlPullParser.END_TAG:
                processEndElement();
                return;
            // break;
            case XmlPullParser.COMMENT:
                // skip comments
                continue;
            // return;
            case XmlPullParser.PROCESSING_INSTRUCTION:
                // skip PIs
                continue;
            // return;
            // break;
            case XmlPullParser.START_DOCUMENT:
                throw new XmlPullParserException(
                        "Unexpected START_DOCUMENT: " + type);
            // break;
            case XmlPullParser.END_DOCUMENT:
                throw new XmlPullParserException(
                        "Unexpected END_DOCUMENT: " + type);
            // break;
            default:
                throw new XmlPullParserException("UNIMPLEMENTED: " + type);
            }
        }
    }

    /**
     * @throws XmlPullParserException
     * @throws IOException
     * 
     */
    private void processMalformedEntityRef()
            throws XmlPullParserException, IOException {
        // handle unresolved entity exceptions
        if (config.isUnresolvedEntityIgnore()) {
            return;
        } else if (config.isUnresolvedEntityReplace()) {
            String name = getCurrentTextCharactersString();
            logger.fine("name=" + name);
            String replacement = Configuration.UNRESOLVED_ENTITY_REPLACEMENT_PREFIX
                    + name
                    + Configuration.UNRESOLVED_ENTITY_REPLACEMENT_SUFFIX;
            current.write(replacement);
            return;
        }
        throw new XmlPullParserException("Unresolved entity error at "
                + xpp.getPositionDescription());
    }

    /**
     * @return
     */
    private String getCurrentTextCharactersString() {
        int[] sl = new int[2];
        char[] chars = xpp.getTextCharacters(sl);
        char[] nameChars = new char[sl[1]];
        System.arraycopy(chars, sl[0], nameChars, 0, sl[1]);
        return new String(nameChars);
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

    private void processStartElement() throws IOException,
            XmlPullParserException, XDBCException {
        String name = xpp.getName();
        String namespace = xpp.getNamespace();
        logger.finest(name + " in '" + namespace + "'");

        // TODO preserve default namespace and prefix declarations
        if (rootName == null) {
            // this must be the document root
            rootName = name;
            rootNamespace = namespace;
            logger.fine("found document root: '" + rootName + "' in '"
                    + rootNamespace + "'");
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
            skippingRecord = false;

            // handle automatic id generation here
            boolean useAutomaticIds = config.isUseAutomaticIds();
            if (useAutomaticIds || idName.startsWith("@")) {
                String id = null;
                if (useAutomaticIds) {
                    // automatic ids, starting from 1
                    // monitor uses a synchronized timer
                    id = "" + (1 + monitor.getEventCount());
                    logger.fine("automatic document id " + id);
                } else {
                    // if the idName starts with @, it's an attribute
                    // handle attributes as idName
                    if (xpp.getAttributeCount() < 1) {
                        throw new XmlPullParserException(
                                "found no attributes for recordName = "
                                        + recordName + ", idName="
                                        + idName + " at "
                                        + xpp.getPositionDescription());
                    }
                    // try with and without a namespace: first, try without
                    id = xpp.getAttributeValue("", idName.substring(1));
                    if (id == null) {
                        id = xpp.getAttributeValue(recordNamespace,
                                idName.substring(1));
                    }
                    if (id == null) {
                        throw new XmlPullParserException("null id "
                                + idName + " at "
                                + xpp.getPositionDescription());
                    }
                    logger.fine("found id " + idName + " = " + id);
                }

                String uri = composeUri(id);
                if (checkStartId(id)) {
                    skippingRecord = true;
                    return;
                }

                if (checkExistingUri(uri)) {
                    skippingRecord = true;
                    return;
                }

                composeDocOptions(id);
                current = new OutputDocument(logger, conn, uri, docOpts);
            } else {
                // no known uri, as yet
                current = new OutputDocument(logger);
            }
        }

        // allow for repeated idName elements: use the first one we see, for
        // each recordName
        String text = xpp.getText();
        if (current != null && !current.hasUri() && name.equals(idName)) {
            // pick out the contents and use it for the uri
            if (xpp.next() != XmlPullParser.TEXT)
                throw new XmlPullParserException(
                        "badly formed xml: missing id at "
                                + xpp.getPositionDescription());
            String id = xpp.getText();
            String uri = composeUri(id);

            if (checkStartId(id) || checkExistingUri(uri)) {
                skippingRecord = true;
                return;
            }

            composeDocOptions(id);
            current.open(conn, uri, docOpts);

            // now we know that we'll use this content and id
            current.write(text);
            current.write(id);

            // advance xpp to the END_ELEMENT - brittle?
            if (xpp.next() != XmlPullParser.END_TAG) {
                throw new XmlPullParserException(
                        "badly formed xml: no END_TAG after id text"
                                + xpp.getPositionDescription());
            }
            text = xpp.getText();
            logger.finest("END_TAG = " + text);
            current.write(text);
            return;
        }

        // if the startId is still defined, and the uri has been found,
        // we should skip as much of this work as possible
        // this avoids OutOfMemory errors, too
        if (startId != null && current != null && current.hasUri()) {
            return;
        }

        // ok, we seem to be inside a record
        // check to make sure!
        if (current == null) {
            // silently skip element in a skipped record
            if (skippingRecord) {
                return;
            }
            if (config.isIgnoreUnknown()) {
                logger.warning("skipping unknown non-record element: "
                        + xpp.getName());
                return;
            }
            throw new XmlPullParserException(
                    "unknown non-record element: " + xpp.getName());
        }

        // this seems to be the only way to handle empty elements:
        // write it as a end-element, only.
        // note that attributes are still ok in this case
        if (xpp.isEmptyElementTag()) {
            return;
        }

        current.write(text);
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
     * @param uri
     * @return
     * @throws XDBCException
     */
    private boolean checkExistingUri(String uri) throws XDBCException {
        // return true if we're supposed to check,
        // and if the document already exists
        if (config.isSkipExisting() || config.isErrorExisting()) {
            boolean exists = conn.checkFile(uri);
            logger.fine("checking for uri " + uri + " = " + exists);
            if (exists) {
                if (config.isErrorExisting()) {
                    throw new XDBCException(
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

    private String composeUri(String id) throws XmlPullParserException {
        if (id == null) {
            throw new XmlPullParserException("id may not be null");
        }

        String cleanId = id.trim();
        if (cleanId.length() < 1) {
            throw new XmlPullParserException("id may not be empty");
        }

        // automatically use the current file, if available
        return config.getUriPrefix()
                + ((currentFileBasename == null || currentFileBasename
                        .equals("")) ? "" : currentFileBasename + "/")
                + cleanId + config.getUriSuffix();
    }

    private void processEndElement() throws IOException,
            XmlPullParserException, XDBCException {
        String name = xpp.getName();
        // ignore if no current element has been set
        if (current == null) {
            logger.finest("skipping end element "
                    + name
                    + ": no current record");
            return;
        }

        String namespace = xpp.getNamespace();
        // we should never get this far unless recordName has been set
        if (recordName == null) {
            throw new XmlPullParserException(
                    "end of element before recordName was set");
        }

        // record the element text
        current.write(xpp.getText());

        if (!(recordName.equals(name) && recordNamespace
                .equals(namespace))) {
            // not the end of the record: go look for more nodes
            return;
        }

        // end of record: were we skipping?
        if (skippingRecord) {
            logger.fine("reached the end of skipped record");
            // count it anyway
            monitor.add(null, event);
            closeCurrentRecord();
            return;
        }

        // finish the database document, if appropriate
        if (startId != null) {
            if (current != null) {
                logger.fine("ignoring end of record for "
                        + current.getUri() + ": START_ID " + startId
                        + " not yet found");
            }
            if (current != null) {
                current.abort();
            }
            closeCurrentRecord();
            return;
        }
        if (current == null || !current.hasUri()) {
            throw new XmlPullParserException(
                    "end of record element with no id found: "
                            + Configuration.ID_NAME_KEY + "=" + idName);
        }
        current.flush();

        while (true) {
            try {
                current.commit();
                break;
            } catch (XDBCXQueryException e) {
                e.printStackTrace();
                logger.logException(e.getMessage(), e);
                if (!e.getRetryable()) {
                    logger.warning("non-retryable exception!");
                    throw e;
                }
                logger.warning("sleeping before retry");
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e1) {
                    logger.logException(
                            "interrupted while sleeping in retry", e1);
                }
            }
        }
        logger.fine("commit ok for " + current.getUri());

        // done: clean up for the next record
        event.increment(current.getBytesWritten());
        monitor.add(current.getUri(), event);
        closeCurrentRecord();
    }

    private void closeCurrentRecord() throws IOException {
        if (current != null) {
            current.close();
        }
        current = null;
        event = null;
        skippingRecord = false;
    }

    private void processText() throws XmlPullParserException, IOException {
        if (current == null) {
            return;
        }

        // if the startId is still defined, and the uri has been found,
        // we should skip as much of this work as possible
        // this avoids OutOfMemory too
        if (startId != null && current.getUri() != null) {
            return;
        }

        String text = xpp.getText();

        if (xpp.getEventType() == XmlPullParser.TEXT)
            text = Utilities.escapeXml(text);

        // logger.finest("processText = " + text);
        // logger.finest("processText = " + Utilities.dumpHex(text,
        // inputEncoding));
        current.write(text);
    }

    /**
     * @param _logger
     */
    public static void setLogger(SimpleLogger _logger) {
        logger = _logger;
    }

    /**
     * @param _map
     */
    public void setCollectionMap(Map _map) {
        collectionMap = _map;
    }

}
