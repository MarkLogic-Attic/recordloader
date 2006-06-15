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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.MalformedInputException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

import com.marklogic.ps.timing.TimedEvent;
import com.marklogic.ps.timing.Timer;
import com.marklogic.xdbc.XDBCException;
import com.marklogic.xdbc.XDBCXQueryException;
import com.marklogic.xdmp.XDMPDocInsertStream;
import com.marklogic.xdmp.XDMPDocOptions;

/**
 * @author Michael Blakeley, <michael.blakeley@marklogic.com>
 * 
 */

public class RecordLoader extends Thread {

    /**
     * 
     */
    private static final String SIMPLE_NAME = RecordLoader.class
            .getSimpleName();

    public static final String VERSION = "2006-06-15.2";

    static final String NAME = RecordLoader.class.getName();

    private static SimpleLogger logger = SimpleLogger.getSimpleLogger();

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

    private Timer timer;

    private TimedEvent recordEvent;

    private XDMPDocOptions docOpts;

    private RecordLoaderOutputDocument current = null;

    private String currentFileBasename = null;

    private static long lastDisplayMillis = 0;

    private long totalSkipped = 0;

    private Map collectionMap;

    private RecordLoaderConfiguration config;

    /**
     * @param _connString
     * @param _idName
     * @param _config
     * @param _reader
     * @param _timer
     * @throws XDBCException
     * @throws XmlPullParserException
     */
    public RecordLoader(String _connString, String _idName,
            RecordLoaderConfiguration _config, Reader _reader,
            Timer _timer) throws XDBCException, XmlPullParserException {
        idName = _idName;
        config = _config;
        initialize(_connString, _idName, _reader, _timer);
    }

    private void initialize(String _connString, String _idName,
            Reader _reader, Timer _timer) throws XmlPullParserException,
            XDBCException {
        // error if null, automatically
        conn = new Connection(_connString);

        logger = config.getLogger();

        // error if null
        idName = _idName;
        if (idName == null) {
            throw new XmlPullParserException(
                    "Missing required property: "
                            + RecordLoaderConfiguration.ID_NAME_KEY);
        }
        logger.fine(RecordLoaderConfiguration.ID_NAME_KEY + "=" + idName);

        // cache certain info locally
        recordName = config.getRecordName();
        recordNamespace = config.getRecordNamespace();
        startId = config.getStartId();

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
        if (_reader != null) {
            xpp.setInput(new BufferedReader(_reader));
        }

        // handle timer
        timer = _timer;
    }

    public static void main(String[] args) throws Throwable {
        // use system properties as a basis
        // this allows any number of properties at the command-line,
        // using -DPROPNAME=foo
        // as a result, we no longer need any args: default to stdin
        RecordLoaderConfiguration config = new RecordLoaderConfiguration();
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

        Timer rlTimer;
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

        rlTimer = new Timer();
        if (zipFiles.size() > 0 || xmlFiles.size() > 0) {
            handleFileInput(config, xmlFiles, zipFiles, inputDecoder,
                    rlTimer);
        } else {
            handleStandardInput(config, inputDecoder, rlTimer);
        }

        finishMain(rlTimer);
    }

    private static CharsetDecoder getDecoder(String inputEncoding,
            String malformedInputAction) {
        CharsetDecoder inputDecoder;
        logger.info("using input encoding " + inputEncoding);
        // using an explicit decoder allows us to control the error reporting
        inputDecoder = Charset.forName(inputEncoding).newDecoder();
        if (malformedInputAction
                .equals(RecordLoaderConfiguration.INPUT_MALFORMED_ACTION_IGNORE)) {
            inputDecoder.onMalformedInput(CodingErrorAction.IGNORE);
        } else if (malformedInputAction
                .equals(RecordLoaderConfiguration.INPUT_MALFORMED_ACTION_REPLACE)) {
            inputDecoder.onMalformedInput(CodingErrorAction.REPLACE);
        } else {
            inputDecoder.onMalformedInput(CodingErrorAction.REPORT);
        }
        logger.info("using malformed input action "
                + inputDecoder.unmappableCharacterAction().toString());
        inputDecoder.onUnmappableCharacter(CodingErrorAction.REPORT);
        return inputDecoder;
    }

    private static Timer handleFileInput(
            RecordLoaderConfiguration _config, List<File> xmlFiles,
            List<File> zipFiles, CharsetDecoder inputDecoder,
            Timer rlTimer) throws IOException, ZipException,
            FileNotFoundException, XDBCException, XmlPullParserException,
            InterruptedException {
        File file = null;
        int connStringIndex = 0;
        String[] connectionStrings = _config.getConnectionStrings();
        int connStringLength = connectionStrings.length;
        // use a global timer for all files
        RecordLoader[] threads = new RecordLoader[_config
                .getThreadCount()];
        ZipFile currentZipFile = null;
        Enumeration currentZipEntries = null;
        ZipEntry ze = null;
        InputStream zis = null;
        Reader theReader = null;
        boolean active = true;
        String baseName;
        // to avoid closing zipinputstreams randomly,
        // we have to "leak" them temporarily
        List<ZipFile> openZipFiles = new ArrayList<ZipFile>(zipFiles.size());
        // we want to wait around whenever threads are busy,
        // and add more work as needed.
        // TODO if START_ID was supplied, run single-threaded until found
        while (active) {
            logger.finest("active loop starting");
            // spawn more threads, if needed
            for (int i = 0; i < threads.length; i++) {
                logger.finest("active loop, thread " + i);
                // is there any work left to do?
                // be sure to check zis from our last iteration, too
                if (zis == null && zipFiles.size() < 1
                        && xmlFiles.size() < 1) {
                    active = false;
                    break;
                }

                if (threads[i] != null && threads[i].isAlive()) {
                    // this thread is already working: skip it
                    continue;
                }

                // preferentially handle zipfiles first
                // as a side effect, getNextZipEntry sets 'file'
                zis = null;
                while (zis == null) {
                    // scan for an entry to use
                    if (currentZipFile == null
                            || currentZipEntries == null
                            || !currentZipEntries.hasMoreElements()) {
                        if (zipFiles.size() < 1) {
                            // we have no more entries,
                            // and no more files to check
                            // still can't close the zipfiles!
                            break;
                        }
                        file = (zipFiles.remove(0));
                        if (currentZipFile != null) {
                            // do not close: zipfile may have open streams!
                            //currentZipFile.close();
                            // do we have to leak it? try it...
                            openZipFiles.add(currentZipFile);
                        }
                        currentZipFile = new ZipFile(file);
                        currentZipEntries = currentZipFile.entries();
                        logger.fine("current zip file is "
                                + file.getCanonicalPath());
                    }
                    ze = (ZipEntry) currentZipEntries.nextElement();
                    logger.fine("found zip entry " + ze);
                    if (ze.isDirectory()) {
                        // go back and try again
                        continue;
                    }
                    logger.fine("opening input stream for " + ze);
                    zis = currentZipFile.getInputStream(ze);
                }

                // did we get a zipentry to process?
                logger.fine("zis = " + zis);
                if (zis != null) {
                    // now we have a valid ze that isn't a directory
                    logger.info("loading from " + file.getCanonicalPath()
                            + ", zip entry " + ze.getName());
                    theReader = new InputStreamReader(zis, inputDecoder);
                    // we might use the entryName, or both,
                    // but that causes too many problems
                    baseName = stripExtension(file.getName());
                } else {
                    // try for a file instead
                    if (xmlFiles.size() < 1) {
                        // looks like we're done... skip this thread and try
                        // again, just to make sure.
                        continue;
                    }
                    file = (xmlFiles.remove(0));
                    logger
                            .info("loading from "
                                    + file.getCanonicalPath());
                    theReader = new InputStreamReader(
                            new FileInputStream(file), inputDecoder);
                    baseName = stripExtension(file.getName());
                }
                threads[i] = new RecordLoader(
                        connectionStrings[connStringIndex], _config
                                .getIdNodeName(), _config, theReader,
                        rlTimer);
                // if multiple connString are available, we round-robin
                connStringIndex++;
                connStringIndex = connStringIndex % connStringLength;

                // strip the extension too
                threads[i].setFileBasename(baseName);
                threads[i].start();

            } // for threads

            // all the threads are busy: sleep a while
            logger.fine("active loop sleeping "
                    + RecordLoaderConfiguration.SLEEP_TIME + " ms");
            Thread.sleep(RecordLoaderConfiguration.SLEEP_TIME);
        } // while active

        // wait for all threads to complete their work
        logger.info("no files remaining");
        for (int i = 0; i < threads.length; i++) {
            if (threads[i] != null) {
                logger.info("waiting for thread " + i);
                threads[i].join();
            }
        }
        
        // clean up a little... should fall out of scope anyhow
        Iterator<ZipFile> iter = openZipFiles.iterator();
        while (iter.hasNext()) {
            iter.next().close();
        }

        return rlTimer;
    }

    private static void handleStandardInput(
            RecordLoaderConfiguration _config,
            CharsetDecoder inputDecoder, Timer rlTimer)
            throws XDBCException, XmlPullParserException, IOException {
        // use stdin by default
        // NOTE: will not use threads
        logger.info("Reading from standard input...");
        if (_config.getThreadCount() > 1) {
            logger.warning("Will not use multiple threads!");
        }
        BufferedReader br = new BufferedReader(new InputStreamReader(
                System.in, inputDecoder));
        RecordLoader rl = new RecordLoader(
                _config.getConnectionStrings()[0], _config
                        .getIdNodeName(), _config, br, rlTimer);
        rl.process();
        br.close();
    }

    private static void finishMain(Timer rlTimer) {
        rlTimer.stop();
        logger.info("loaded " + rlTimer.getEventCount() + " records ok ("
                + rlTimer.getProgressMessage(true) + ")");
    }

    /**
     * @param name
     * @return
     */
    private static String stripExtension(String name) {
        if (name == null || name.length() < 3) {
            return name;
        }

        int i = name.lastIndexOf('.');
        if (i < 1) {
            return name;
        }

        return name.substring(0, i);
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

    public Timer getTimer() {
        return timer;
    }

    public void process() throws XmlPullParserException, IOException,
            XDBCException {
        int eventType;

        try {
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
                        throw new XmlPullParserException(
                                "UNIMPLEMENTED: " + eventType);
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
                        logger.warning("attribute error: "
                                + e.getMessage());
                        // all we can do is ignore it, apparently
                    } else {
                        throw e;
                    }
                } catch (XDBCException e) {
                    if (!config.isFatalErrors()) {
                        // keep going
                        logger.logException("non-fatal: skipping", e);

                        recordEvent.stop(true);
                        timer.add(recordEvent);
                        if (System.currentTimeMillis()
                                - lastDisplayMillis > RecordLoaderConfiguration.DISPLAY_MILLIS) {
                            lastDisplayMillis = System
                                    .currentTimeMillis();
                            displayProgress();
                        }

                        closeCurrentRecord();
                        continue;
                    }
                }
            }

            if (current != null) {
                throw new XmlPullParserException(
                        "end of document before end of current record!\n"
                                + "recordName = " + recordName
                                + ", recordNamespace = "
                                + recordNamespace + " at "
                                + xpp.getPositionDescription() + "\n"
                                + current.getUri());
            }

        } catch (XDBCXQueryException e) {
            logger.info("current uri: " + current.getUri());
            logger.info("current characters: "
                    + getCurrentTextCharactersString());
            throw e;
        } catch (XmlPullParserException e) {
            if (current != null) {
                logger.info("current uri: " + current.getUri());
                logger.info("current characters: "
                        + getCurrentTextCharactersString());
                if (current != null) {
                    logger.info("current record:\n" + current);
                }
            }
            throw e;
        } catch (MalformedInputException e) {
            // invalid character sequence, probably
            logger
                    .warning("input could not be decoded: try setting "
                            + RecordLoaderConfiguration.INPUT_ENCODING_KEY
                            + " (or set "
                            + RecordLoaderConfiguration.INPUT_MALFORMED_ACTION_KEY
                            + " to "
                            + RecordLoaderConfiguration.INPUT_MALFORMED_ACTION_IGNORE
                            + " or "
                            + RecordLoaderConfiguration.INPUT_MALFORMED_ACTION_REPLACE
                            + ").");
            throw e;
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
            String replacement = RecordLoaderConfiguration.UNRESOLVED_ENTITY_REPLACEMENT_PREFIX
                    + name
                    + RecordLoaderConfiguration.UNRESOLVED_ENTITY_REPLACEMENT_SUFFIX;
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
            recordEvent = new TimedEvent();
            skippingRecord = false;

            // handle automatic id generation here
            boolean useAutomaticIds = config.isUseAutomaticIds();
            if (useAutomaticIds || idName.startsWith("@")) {
                String id = null;
                if (useAutomaticIds) {
                    // automatic ids, starting from 1
                    id = "" + (1 + timer.getEventCount());
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
                current = new RecordLoaderOutputDocument(logger, conn,
                        uri, docOpts);
            } else {
                // no known uri, as yet
                current = new RecordLoaderOutputDocument(logger);
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
            if (xpp.next() != XmlPullParser.END_TAG)
                throw new XmlPullParserException(
                        "badly formed xml: no END_TAG after id text"
                                + xpp.getPositionDescription());
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

    private String composeUri(String id) {
        // automatically use the current file, if available
        return config.getUriPrefix()
                + ((currentFileBasename == null || currentFileBasename
                        .equals("")) ? "" : currentFileBasename + "/")
                + id + config.getUriSuffix();
    }

    private void processEndElement() throws IOException,
            XmlPullParserException, XDBCException {
        // ignore if no current element has been set
        if (current == null) {
            logger.finest("skipping end element: no current record");
            return;
        }

        // we should never get this far unless recordName has been set
        if (recordName == null) {
            throw new XmlPullParserException(
                    "end of element before recordName was set");
        }

        // record the element text
        current.write(xpp.getText());

        if (!(recordName.equals(xpp.getName()) && recordNamespace
                .equals(xpp.getNamespace()))) {
            // not the end of the record: go look for more nodes
            return;
        }

        // end of record: were we skipping?
        if (skippingRecord) {
            logger.fine("reached the end of skipped record");
            // count it anyway
            timer.add(recordEvent);
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
        if (current == null) {
            throw new XmlPullParserException(
                    "end of record element with no id found: "
                            + RecordLoaderConfiguration.ID_NAME_KEY + "="
                            + idName);
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

        recordEvent.increment(current.getBytesWritten());
        timer.add(recordEvent);
        if (System.currentTimeMillis() - lastDisplayMillis > RecordLoaderConfiguration.DISPLAY_MILLIS) {
            lastDisplayMillis = System.currentTimeMillis();
            displayProgress();
        }
        // done: clean up for the next record
        closeCurrentRecord();
    }

    private void closeCurrentRecord() throws IOException {
        if (current != null) {
            current.close();
        }
        current = null;
        recordEvent = null;
        skippingRecord = false;
    }

    private void displayProgress() {
        logger.info("inserted record " + timer.getEventCount() + " as "
                + current.getUri() + " (" + timer.getProgressMessage()
                + ")");
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

    public void run() {
        try {
            process();
        } catch (Exception e) {
            logger.logException(
                    "fatal error in file "
                            + currentFileBasename
                            + " at "
                            + (xpp == null ? null : xpp
                                    .getPositionDescription()), e);
        }
    }

    /**
     * @param _logger
     */
    public void setLogger(SimpleLogger _logger) {
        logger = _logger;
    }

    /**
     * @param _map
     */
    public void setCollectionMap(Map _map) {
        collectionMap = _map;
    }

}
