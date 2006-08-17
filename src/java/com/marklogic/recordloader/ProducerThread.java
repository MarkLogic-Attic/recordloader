/**
 * Copyright (c) 2006 Mark Logic Corporation. All rights reserved.
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
import java.io.OutputStream;

import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

import com.marklogic.ps.SimpleLogger;
import com.marklogic.ps.Utilities;
import com.marklogic.xcc.exceptions.UnimplementedFeatureException;
import com.marklogic.xcc.exceptions.XccException;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class ProducerThread extends Thread {

    private String outputEncoding = Configuration.OUTPUT_ENCODING_DEFAULT;

    private SimpleLogger logger;

    private XmlPullParser xpp;

    private OutputStream stream;

    private String recordName;

    private String recordNamespace;

    private StringBuffer prepend;

    private Loader loader;

    private Configuration config;

    private String idName;

    private boolean skippingRecord = false;

    private Exception lastException;

    private long bytesWritten = 0;

    private String currentId;

    private Thread loaderThread;

    private Object outputMutex = new Object();

    /**
     * @param _loader
     * @param _config
     * @param _logger
     * @param _xpp
     */
    public ProducerThread(Loader _loader, Configuration _config,
            SimpleLogger _logger, XmlPullParser _xpp) {
        loader = _loader;
        config = _config;
        logger = _logger;
        xpp = _xpp;

        recordNamespace = config.getRecordNamespace();
        recordName = config.getRecordName();
        logger.fine("recordName=" + recordName);

        prepend = new StringBuffer();
    }

    public void run() {
        try {
            // by definition, we are at the start of an element:
            // keep processing until we reach its end.
            idName = config.getIdNodeName();
            handleRecordStart();
            process();
            if (currentId == null) {
                throw new XmlPullParserException("no id found");
            }
            if (stream != null) {
                logger.fine("cleaning up");
                stream.flush();
                stream.close();
            }
        } catch (Exception e) {
            logger.logException("caught exception", e);
            lastException = e;
        }
        logger.fine("exiting");
        // loader.notify();
        return;
    }

    /**
     * @throws XmlPullParserException
     * @throws IOException
     * @throws XccException
     * 
     */
    private void handleRecordStart() throws XmlPullParserException,
            XccException, IOException {

        // handle automatic id generation here
        boolean useAutomaticIds = config.isUseAutomaticIds();
        boolean useFileNameIds = config.isUseFileNameIds();
        if (useAutomaticIds || useFileNameIds || idName.startsWith("@")) {
            if (useAutomaticIds) {
                // automatic ids, starting from 1
                // config uses a synchronized sequence of long
                currentId = config.getAutoId();
                logger.fine("automatic document id " + currentId);
            } else if (useFileNameIds) {
                // the constructor had better have set our id!
                if (currentId == null) {
                    throw new UnimplementedFeatureException(
                            "Cannot use filename ids unless the constructor sets currentId");
                }
                logger.fine("using filename id " + currentId);
            } else {
                // if the idName starts with @, it's an attribute
                // handle attributes as idName
                if (xpp.getAttributeCount() < 1) {
                    throw new XmlPullParserException(
                            "found no attributes for recordName = "
                                    + recordName + ", idName=" + idName
                                    + " at "
                                    + xpp.getPositionDescription());
                }
                // try with and without a namespace: first, try without
                currentId = xpp
                        .getAttributeValue("", idName.substring(1));
                if (currentId == null) {
                    currentId = xpp.getAttributeValue(recordNamespace,
                            idName.substring(1));
                }
                if (currentId == null) {
                    throw new XmlPullParserException("null id " + idName
                            + " at " + xpp.getPositionDescription());
                }
                logger.fine("found id " + idName + " = " + currentId);
            }

            if (loader.checkId(currentId)) {
                // for whatever reason, the loader wants us to skip this id
                skippingRecord = true;
                return;
            }

            logger.finer("notifying loader of currentId = " + currentId);
            synchronized (loader) {
                loader.notify();
            }
        }

        // write the current tag
        processStartElement(true);
    }

    public void process() throws XmlPullParserException, IOException,
            XccException {
        int eventType;

        // NOTE: next() skips comments, document-decl, ignorable-whitespace,
        // processing-instructions automatically.
        // to catch these, use nextToken() instead.
        // nextToken() could also be used for custom entity handling.
        boolean c = true;
        while (c) {
            try {
                eventType = xpp.next();
                logger.finer("eventType = " + eventType);
                switch (eventType) {
                case XmlPullParser.START_TAG:
                    processStartElement();
                    break;
                case XmlPullParser.TEXT:
                    processText();
                    break;
                case XmlPullParser.END_TAG:
                    c = processEndElement();
                    break;
                case XmlPullParser.START_DOCUMENT:
                    throw new XmlPullParserException(
                            "unexpected start of document within record!\n"
                                    + "recordName = " + recordName
                                    + ", recordNamespace = "
                                    + recordNamespace + " at "
                                    + xpp.getPositionDescription());
                case XmlPullParser.END_DOCUMENT:
                    throw new XmlPullParserException(
                            "end of document before end of current record!\n"
                                    + "recordName = " + recordName
                                    + ", recordNamespace = "
                                    + recordNamespace + " at "
                                    + xpp.getPositionDescription());
                default:
                    throw new XmlPullParserException("UNIMPLEMENTED: "
                            + eventType);
                }
            } catch (XmlPullParserException e) {
                logger.warning(e.getClass().getSimpleName() + " at "
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
            }
        }

    }

    private void processText() throws XmlPullParserException, IOException {
        // if the startId is still defined, and the uri has been found,
        // we should skip as much of this work as possible
        // this avoids OutOfMemory too
        if (skippingRecord) {
            return;
        }

        String text = xpp.getText();

        if (xpp.getEventType() == XmlPullParser.TEXT) {
            text = Utilities.escapeXml(text);
        }

        write(text);
    }

    private void processStartElement(boolean copyNamespaceDeclarations)
            throws IOException, XmlPullParserException, XccException {
        String name = xpp.getName();
        // String namespace = xpp.getNamespace();
        logger.finest("name = " + name);
        String text = xpp.getText();

        // allow for repeated idName elements: use the first one we see, for
        // each recordName
        // NOTE: idName is namespace-ignorant
        if (currentId == null && name.equals(idName)) {
            // pick out the contents and use it for the uri
            if (xpp.next() != XmlPullParser.TEXT) {
                throw new XmlPullParserException(
                        "badly formed xml: missing id at "
                                + xpp.getPositionDescription());
            }

            currentId = xpp.getText();
            logger.fine("found id " + idName + " = " + currentId);

            if (loader.checkId(currentId)) {
                // for whatever reason, the loader wants us to skip this id
                skippingRecord = true;
                return;
            }

            loaderThread.interrupt();

            // now we know that we'll use this content and id
            write(text);
            write(currentId);

            // advance xpp to the END_ELEMENT - brittle?
            if (xpp.next() != XmlPullParser.END_TAG) {
                throw new XmlPullParserException(
                        "badly formed xml: no END_TAG after id text"
                                + xpp.getPositionDescription());
            }
            text = xpp.getText();
            logger.finest("END_TAG = " + text);
            write(text);
            return;
        }

        // if the startId is still defined, and the uri has been found,
        // we should skip as much of this work as possible
        // this avoids OutOfMemory errors, too
        if (skippingRecord) {
            return;
        }

        // this seems to be the only way to handle empty elements:
        // write it as a end-element, only.
        // note that attributes are still ok in this case
        boolean isEmpty = xpp.isEmptyElementTag();
        if (isEmpty) {
            logger.finest("empty element");
            return;
        }

        if (copyNamespaceDeclarations) {
            // preserve namespace declarations into this element
            int stop = xpp.getNamespaceCount(xpp.getDepth());
            if (stop > 0) {
                StringBuffer decl = null;
                String nsDeclPrefix, nsDeclUri;
                logger.finer("checking namespace declarations");
                for (int i = 0; i < stop; i++) {
                    if (decl == null) {
                        decl = new StringBuffer();
                    } else {
                        decl.append(" ");
                    }
                    nsDeclPrefix = xpp.getNamespacePrefix(i);
                    nsDeclUri = xpp.getNamespaceUri(i);
                    logger.finest("found namespace declaration "
                            + nsDeclPrefix + " = " + nsDeclUri);
                    decl.append("xmlns"
                            + (nsDeclPrefix == null ? ""
                                    : (":" + nsDeclPrefix)) + "=\""
                            + nsDeclUri + "\"");
                }
                // copy the namespace decls to the end of the tag
                if (decl != null) {
                    logger.finer("copying namespace declarations");
                    text = text.replaceFirst(">$", decl.toString()
                            + (isEmpty ? "/" : "") + ">");
                }
            } else {
                logger.finer("no namespace declarations to copy");
            }
        }

        logger.finer("writing text");
        write(text);
    } // NOTE: must return false when the record end-element is found

    private boolean processEndElement() throws IOException,
            XmlPullParserException {
        String name = xpp.getName();
        String namespace = xpp.getNamespace();
        logger.finest("name = " + name);

        // record the element text
        write(xpp.getText());
        while (currentId != null && stream == null) {
            synchronized (loader) {
                loader.notify();
            }
            yield();
        }
        if (stream != null) {
            stream.flush();
        }

        if (!(recordName.equals(name) && recordNamespace
                .equals(namespace))) {
            // not the end of the record: go look for more nodes
            return true;
        }

        // end of record: were we skipping?
        if (skippingRecord) {
            logger.fine("reached the end of skipped record");
            return false;
        }

        // did something go wrong?
        if (currentId == null) {
            throw new XmlPullParserException("end of record element "
                    + name + " with no id found: "
                    + Configuration.ID_NAME_KEY + "=" + idName);
        }

        // end of record
        logger.fine("end of record");
        return false;
    }

    /**
     * @return
     */
    public Exception getException() {
        return lastException;
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
            write(replacement);
            return;
        }
        throw new XmlPullParserException("Unresolved entity error at "
                + xpp.getPositionDescription());
    }

    /**
     * @param string
     * @throws IOException
     */
    private void write(String string) throws IOException {
        synchronized (outputMutex) {
            if (stream != null && prepend != null) {
                throw new IOException(
                        "prepend and stream are both active");
            }

            if (stream != null) {
                // logger.finest("writing to stream " + bytesWritten);
                byte[] bytes = string.getBytes(outputEncoding);
                stream.write(bytes);
                bytesWritten += bytes.length;
                return;
            }

            if (prepend != null) {
                prepend.append(string);
                return;
            }
        }

        throw new IOException("prepend and stream are both null");
    }

    /**
     * @throws IOException
     * @throws XmlPullParserException
     * @throws XccException
     * 
     */
    private void handleUnresolvedEntity() throws XmlPullParserException,
            IOException, XccException {
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
     * @throws XccException
     * @throws XmlPullParserException
     * @throws IOException
     * 
     */
    private void processStartElement() throws IOException,
            XmlPullParserException, XccException {
        processStartElement(false);
    }

    /**
     * @return
     */
    public long getBytesWritten() {
        return bytesWritten;
    }

    public String getCurrentId() {
        return currentId;
    }

    /**
     * @param outputStream
     * @throws IOException
     */
    public void setOutputStream(OutputStream outputStream)
            throws IOException {
        synchronized (outputMutex) {
            stream = outputStream;
            // handle any prepend
            if (prepend != null) {
                String prependString = prepend.toString();
                prepend = null;
                write(prependString);
            }
        }
    }

    /**
     * @param thread
     */
    public void setLoaderThread(Thread thread) {
        loaderThread = thread;
    }

    public void setCurrentId(String currentId) {
        this.currentId = currentId;
    }

}
