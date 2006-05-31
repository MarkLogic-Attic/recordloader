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
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlPullParserFactory;

import com.marklogic.ps.timing.TimedEvent;
import com.marklogic.ps.timing.Timer;
import com.marklogic.ps.timing.TimerEventException;
import com.marklogic.xdbc.XDBCException;
import com.marklogic.xdbc.XDBCResultSequence;
import com.marklogic.xdbc.XDBCXQueryException;
import com.marklogic.xdmp.XDMPDocInsertStream;
import com.marklogic.xdmp.XDMPDocOptions;
import com.marklogic.xdmp.XDMPPermission;

/**
 * @author Michael Blakeley, <michael.blakeley@marklogic.com>
 * 
 * example: -DCONNECTION_STRING=admin:admin@localhost:9015 -DID_NAME=@UID
 * -DRECORD_NAME=Record -DIGNORE_UNKNOWN=true -DCOLLECTIONS=psycinfo
 * -DURI_PREFIX=/psycinfo/main/ -DURI_SUFFIX=.xml -DLOG_LEVEL=FINEST
 * -DLOG_HANDLER=CONSOLE
 */

public class RecordLoader extends Thread {

    /**
     * 
     */
    public static final String CONNECTION_STRING_KEY = "CONNECTION_STRING";

    /**
     * 
     */
    public static final String INPUT_PATTERN_KEY = "INPUT_PATTERN";

    /**
     * 
     */
    public static final String INPUT_PATH_KEY = "INPUT_PATH";

    /**
     * 
     */
    public static final String DEFAULT_NAMESPACE_KEY = "DEFAULT_NAMESPACE";

    /**
     * 
     */
    public static final String ERROR_EXISTING_KEY = "ERROR_EXISTING";

    /**
     * 
     */
    public static final String SKIP_EXISTING_KEY = "SKIP_EXISTING";

    /**
     * 
     */
    public static final String OUTPUT_URI_SUFFIX_KEY = "URI_SUFFIX";

    /**
     * 
     */
    public static final String START_ID_KEY = "START_ID";

    /**
     * 
     */
    public static final String OUTPUT_URI_PREFIX_KEY = "URI_PREFIX";

    /**
     * 
     */
    public static final String THREADS_KEY = "THREADS";

    /**
     * 
     */
    public static final String ID_NAME_KEY = "ID_NAME";

    /**
     * 
     */
    public static final String OUTPUT_COLLECTIONS_KEY = "COLLECTIONS";

    /**
     * 
     */
    public static final String IGNORE_UNKNOWN_KEY = "IGNORE_UNKNOWN";

    /**
     * 
     */
    public static final String RECORD_NAMESPACE_KEY = "RECORD_NAMESPACE";

    /**
     * 
     */
    public static final String RECORD_NAME_KEY = "RECORD_NAME";

    /**
     * 
     */
    public static final String OUTPUT_FORESTS_KEY = "OUTPUT_FORESTS";

    private static final int DISPLAY_MILLIS = 3000;

    private static final int SLEEP_TIME = 500;

    private static final String NAME = RecordLoader.class.getName();

    public static final String OUTPUT_READ_ROLES_KEY = "READ_ROLES";

    private static final String VERSION = "2006-05-30.1";

    private static SimpleLogger logger = SimpleLogger.getSimpleLogger();

    private XmlPullParser xpp = null;

    // current record
    private XDMPDocInsertStream current = null;

    private String rootName;

    private String rootNamespace;

    private String recordName;

    private String recordNamespace;

    private Connection conn;

    private Properties props;

    private String uri;

    private String idName;

    private XmlPullParserFactory factory;

    private boolean ignoreUnknown;

    private String uriPrefix = "";

    private String uriSuffix = "";

    private String startId = null;

    private Timer timer;

    private TimedEvent recordEvent;

    private XDMPDocOptions docOpts;

    private StringBuffer currentPrepend;

    private boolean skipExisting = false;

    private boolean errorExisting = false;

    private boolean skippingRecord = false;

    private String currentFileBasename = null;

    private static long lastDisplayMillis = 0;

    private String[] baseCollections;

    private long totalSkipped = 0;

    private Map collectionMap;

    /**
     * @param _connString
     * @param _idName
     * @param _reader
     * @throws XDBCException
     * @throws XmlPullParserException
     */
    public RecordLoader(String _connString, String _idName, Properties _props,
            Reader _reader) throws XDBCException, XmlPullParserException {
        idName = _idName;
        initialize(_connString, _idName, _props, _reader, null);
    }

    /**
     * @param _connString
     * @param _idName
     * @param _reader
     * @param _timer
     * @throws XDBCException
     * @throws XmlPullParserException
     */
    public RecordLoader(String _connString, String _idName, Reader _reader,
            Properties _props, Timer _timer) throws XDBCException,
            XmlPullParserException {
        initialize(_connString, _idName, _props, _reader, _timer);
    }

    /**
     * @param connString
     * @param _idName
     * @param _reader
     * @param _props
     * @param _timer
     * @throws XDBCException
     * @throws XmlPullParserException
     */
    public RecordLoader(URI _uri, String _idName, FileReader _reader,
            Properties _props, Timer _timer) throws XmlPullParserException,
            XDBCException {
        // support XCC uris, for forward compatibility
        String connString = _uri.toString();
        connString = connString.replaceFirst("^(xdbc://)?([^/]+)(/.*)?$", "$2");
        initialize(connString, _idName, _props, _reader, _timer);
    }

    private void initialize(String _connString, String _idName,
            Properties _props, Reader _reader, Timer _timer)
            throws XmlPullParserException, XDBCException {
        // error if null, automatically
        conn = new Connection(_connString);

        // error if null (later)
        idName = _idName;
        if (idName == null) {
            throw new XmlPullParserException(
                    "Missing required property: ID_NAME");
        }

        props = _props;
        if (props == null) {
            props = new Properties();
        }

        // some or all of these may be null
        recordName = props.getProperty(RECORD_NAME_KEY);
        recordNamespace = props.getProperty(RECORD_NAMESPACE_KEY);
        if (recordName != null && recordNamespace == null)
            recordNamespace = "";

        ignoreUnknown = Utilities.stringToBoolean(props.getProperty(
                IGNORE_UNKNOWN_KEY, "false"));

        // initialize collections
        List collections = new ArrayList();
        collections.add(NAME + "." + System.currentTimeMillis());
        logger.info("adding extra collection: " + collections.get(0));
        String collectionsString = props.getProperty(OUTPUT_COLLECTIONS_KEY);
        if (collectionsString != null && !collectionsString.equals("")) {
            collections.addAll(Arrays.asList(collectionsString.split(",")));
        }

        // use prefix to set document-uri patterns
        uriPrefix = props.getProperty(OUTPUT_URI_PREFIX_KEY, "");
        if (!uriPrefix.equals("") && !uriPrefix.endsWith("/")) {
            uriPrefix += "/";
        }
        uriSuffix = props.getProperty(OUTPUT_URI_SUFFIX_KEY, "");

        // look for startId, to skip records
        startId = props.getProperty(START_ID_KEY);
        logger.fine("START_ID=" + startId);

        // should we check for existing docs?
        skipExisting = Utilities.stringToBoolean(props.getProperty(
                SKIP_EXISTING_KEY, "false"));
        logger.fine("SKIP_EXISTING=" + skipExisting);

        // should we throw an error for existing docs?
        errorExisting = Utilities.stringToBoolean(props.getProperty(
                ERROR_EXISTING_KEY, "false"));
        logger.fine("ERROR_EXISTING=" + errorExisting);

        // only initialize docOpts once
        if (docOpts == null) {
            boolean resolveEntities = false;
            XDMPPermission[] permissions = null;
            String readRolesString = props.getProperty(OUTPUT_READ_ROLES_KEY,
                    "");
            if (readRolesString != null && readRolesString.length() > 0) {
                String[] readRoles = readRolesString.trim().split("\\s+");
                if (readRoles != null && readRoles.length > 0) {
                    permissions = new XDMPPermission[readRoles.length];
                    for (int i = 0; i < readRoles.length; i++) {
                        if (readRoles[i] != null && !readRoles[i].equals(""))
                            permissions[i] = new XDMPPermission(
                                    XDMPPermission.READ, readRoles[i]);
                    }
                }
            }
            int format = XDMPDocInsertStream.XDMP_DOC_FORMAT_XML;
            int quality = 0;
            int repair = XDMPDocInsertStream.XDMP_ERROR_CORRECTION_NONE;
            String namespace = props.getProperty(DEFAULT_NAMESPACE_KEY);
            // support placeKeys for Forest placement
            // comma-delimited string, also accept ;:\s
            String[] placeKeys = null;
            String placeKeysString = props.getProperty(OUTPUT_FORESTS_KEY);
            if (placeKeysString != null) {
                placeKeysString = placeKeysString.trim();
                if (!placeKeysString.equals("")) {
                    // numeric keys, so whitespace is enough
                    placeKeys = placeKeysString.split("\\s+");
                }
            }
            String language = null;
            // keep a base list of collections that can be extended later
            baseCollections = (String[]) collections.toArray(new String[0]);
            docOpts = new XDMPDocOptions(Locale.getDefault(), resolveEntities,
                    permissions, baseCollections, quality, namespace, repair,
                    placeKeys, format, language);
        }

        // get a new factory
        if (factory == null) {
            factory = XmlPullParserFactory.newInstance(System
                    .getProperty(XmlPullParserFactory.PROPERTY_NAME), null);
            factory.setNamespaceAware(true);
        }
        xpp = factory.newPullParser();
        if (_reader != null) {
            xpp.setInput(new BufferedReader(_reader));
        }

        // handle timer
        timer = _timer;
        if (timer == null) {
            // start our own timer
            timer = new Timer();
        }
    }

    public static void main(String[] args) throws Throwable {
        System.err.println(RecordLoader.class.getSimpleName()
                + " starting, version " + VERSION);
        checkEnvironment();

        // use system properties as a basis
        // this allows any number of properties at the command-line,
        // using -DPROPNAME=foo
        // as a result, we no longer need any args: default to stdin
        Properties props = new Properties(System.getProperties());
        List xmlFiles = new ArrayList();
        List zipFiles = new ArrayList();
        Iterator iter = Arrays.asList(args).iterator();
        File file = null;
        String arg = null;
        while (iter.hasNext()) {
            arg = (String) iter.next();
            logger.info("processing argument: " + arg);
            file = new File(arg);
            if (!file.exists()) {
                logger.warning("skipping " + arg + ": file does not exist.");
                continue;
            }
            if (!file.canRead()) {
                logger.warning("skipping " + arg + ": file cannot be read.");
                continue;
            }
            if (arg.endsWith(".properties")) {
                // this will override existing properties
                props.load(new FileInputStream(file));
            } else if (arg.endsWith(".zip")) {
                // add to zip list
                zipFiles.add(file);
            } else {
                // add to xml list
                xmlFiles.add(file);
            }
        }

        logger.configureLogger(props);

        // handle multiple connection strings, for load balancing
        String[] connString = props.getProperty(CONNECTION_STRING_KEY,
                "admin:admin@localhost:9000").split("\\s+");
        logger.info("connecting to " + Utilities.join(connString, " "));

        String idElementName = props.getProperty(ID_NAME_KEY, "id");

        setupEncoding(props);

        setupProperties(connString[0], props);

        Timer rlTimer;
        int threadCount = Integer.parseInt(props.getProperty(THREADS_KEY, "1"));

        String inputPath = props.getProperty(INPUT_PATH_KEY);
        String inputPattern = props
                .getProperty(INPUT_PATTERN_KEY, "^.+\\.xml$");
        if (inputPath != null) {
            // find all the files
            FileFinder ff = new FileFinder(inputPath, inputPattern);
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

        if (zipFiles.size() > 0 || xmlFiles.size() > 0) {
            int connStringIndex = 0;
            int connStringLength = connString.length;
            // use a global timer for all files
            rlTimer = new Timer();
            RecordLoader[] threads = new RecordLoader[threadCount];
            ZipFile currentZipFile = null;
            Enumeration currentZipEntries = null;
            ZipEntry ze = null;
            InputStream zis = null;
            Reader theReader = null;
            boolean active = true;
            // we want to wait around whenever threads are busy,
            // and add more work as needed.
            while (active) {
                logger.finest("active loop starting");
                // spawn more threads, if needed
                for (int i = 0; i < threadCount; i++) {
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
                        if (currentZipFile == null || currentZipEntries == null
                                || !currentZipEntries.hasMoreElements()) {
                            if (zipFiles.size() < 1) {
                                // by definition, we have no more
                                // entries,
                                // and no more files to check
                                break;
                            }
                            file = (File) (zipFiles.remove(0));
                            if (currentZipFile != null) {
                                currentZipFile.close();
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
                        theReader = new InputStreamReader(zis);
                    } else {
                        // try for a file instead
                        if (xmlFiles.size() < 1) {
                            // looks like we're done... skip this thread and try
                            // again, just to make sure.
                            continue;
                        }
                        file = (File) (xmlFiles.remove(0));
                        logger.info("loading from " + file.getCanonicalPath());
                        theReader = new FileReader(file);
                    }
                    threads[i] = new RecordLoader(connString[connStringIndex],
                            idElementName, theReader, props, rlTimer);
                    // if multiple connString are available, we round-robin
                    connStringIndex++;
                    connStringIndex = connStringIndex % connStringLength;

                    // strip the extension too
                    threads[i].setFileBasename(stripExtension(file.getName()));
                    threads[i].start();

                } // for threads

                // all the threads are busy: sleep a while
                logger.fine("active loop sleeping " + SLEEP_TIME + " ms");
                Thread.sleep(SLEEP_TIME);
            } // while active

            // wait for all threads to complete their work
            logger.info("no files remaining");
            for (int i = 0; i < threadCount; i++) {
                if (threads[i] != null) {
                    logger.info("waiting for thread " + i);
                    threads[i].join();
                }
            }
        } else {
            // use stdin by default
            // NOTE: will not use threads
            logger.info("Reading from standard input...");
            if (threadCount > 1) {
                logger.warning("Will not use multiple threads!");
            }
            BufferedReader bin = new BufferedReader(new InputStreamReader(
                    System.in));
            RecordLoader rl = new RecordLoader(connString[0], idElementName,
                    props, bin);
            rl.process();
            bin.close();
            rlTimer = rl.getTimer();
        }

        rlTimer.stop();
        logger.info("loaded " + rlTimer.getEventCount() + " records ok ("
                + rlTimer.getDuration() + " ms, " + rlTimer.getBytes() + " B, "
                + Math.round(rlTimer.getEventRate()) + " tps, "
                + Math.round(rlTimer.getThroughput()) + " kB/s" + ")");
    }

    /**
     * @param props2
     * @throws XDBCException
     */
    private static void setupProperties(String _connString, Properties _props)
            throws XDBCException {
        // if we use OUTPUT_FORESTS, we have to query for it!
        String placeKeysString = _props.getProperty(OUTPUT_FORESTS_KEY);
        if (placeKeysString != null && !placeKeysString.equals("")) {
            logger.info("sending output to forest names: " + placeKeysString);
            logger.fine("querying for Forest ids");
            XDBCResultSequence rs = null;
            String query = "define variable $forest-string as xs:string external\n"
                    + "for $fn in tokenize($forest-string, '[,:;\\s]+')\n"
                    + "return xs:string(xdmp:forest($fn))\n";
            // failures here are fatal
            Connection conn = new Connection(_connString);

            Map externs = new Hashtable(1);
            externs.put("forest-string", placeKeysString);
            rs = conn.executeQuery(query, externs);
            List forestIds = new ArrayList();
            while (rs.hasNext()) {
                rs.next();
                forestIds.add(rs.get_String());
            }
            _props.setProperty(OUTPUT_FORESTS_KEY, Utilities.join(forestIds,
                    " "));
            logger.info("sending output to forests ids: "
                    + _props.getProperty(OUTPUT_FORESTS_KEY));
        }

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

    public void setFileBasename(File _file) {
        setFileBasename(stripExtension(_file.getName()));
    }

    /**
     * @param _path
     */
    private void setFileBasename(String _name) {
        currentFileBasename = _name;
        // update collections
        if (currentFileBasename == null) {
            docOpts.setCollections(baseCollections);
        } else {
            List newCollections = new ArrayList(Arrays.asList(baseCollections));
            newCollections.add(_name);
            docOpts.setCollections((String[]) newCollections
                    .toArray(new String[0]));
        }
        logger.info("using fileBasename = " + currentFileBasename);
    }

    private static void setupEncoding(Properties props) {
        // set encoding
        logger.info("default encoding is "
                + System.getProperty("file.encoding"));
        System.setProperty("file.encoding", props.getProperty(
                "DEFAULT_CHARSET", "UTF-8"));
        logger.info("default encoding is now "
                + System.getProperty("file.encoding"));
    }

    public Timer getTimer() {
        return timer;
    }

    private static void checkEnvironment() throws IOException {
        // check for LANG env: should be LANG=en_US.UTF-8 or similar...
        // also: en_UK.UTF-8@euro
        String envLang = System.getenv("LANG");
        if (envLang == null) {
            envLang = "";
        }
        logger.info("environment variable LANG=" + envLang);
        if (!envLang.matches("^.+\\.UTF-8.*$")) {
            System.err.println("bad environment: LANG='" + envLang
                    + "' does not appear to be UTF-8 compliant."
                    + " Try LANG=en_US.UTF-8 (must end with 'UTF-8').");
            System.exit(1);
        }
    }

    public void process() throws XmlPullParserException, IOException,
            XDBCException, TimerEventException {
        int eventType;

        try {
            while ((eventType = xpp.next()) != XmlPullParser.END_DOCUMENT) {
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
                default:
                    throw new XmlPullParserException("UNIMPLEMENTED: "
                            + eventType);
                }
            }

            if (current != null)
                throw new XmlPullParserException(
                        "end of document before end of current record!\n"
                                + "recordName = " + recordName
                                + ", recordNamespace = " + recordNamespace
                                + "\n" + current);

        } catch (XDBCXQueryException e) {
            if (current != null) {
                logger.info("current record:\n" + current);
            }
            throw e;
        } catch (XmlPullParserException e) {
            if (current != null) {
                logger.info("current record:\n" + current);
            }
            throw e;
        }
    }

    // return true to skip, false to process
    private boolean checkStartId(String id) {
        if (startId == null)
            return false;

        // we're still scanning for the startid:
        // is this my cow?
        if (!startId.equals(id)) {
            // don't bother to open the database: skip this record
            logger.info("skipping record " + (++totalSkipped) + " with id "
                    + id + " != " + startId);
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
        logger.finest(name + " in " + namespace);

        // TODO preserve default namespace and prefix declarations,
        // in any element
        if (rootName == null) {
            // this must be the document root
            rootName = name;
            rootNamespace = namespace;
            logger.fine("found document root: '" + rootName + "' in '"
                    + rootNamespace + "'");
            return;
        }

        if (recordName == null) {
            // this must be the record-level element
            recordName = name;
            recordNamespace = namespace;
            logger.fine("autodetected record element: '" + recordName
                    + "' in '" + recordNamespace + "'");
        }

        if (name.equals(recordName) && namespace.equals(recordNamespace)) {
            // start of a new record
            logger.fine("found record element: '" + recordName + "' in '"
                    + recordNamespace + "'");
            recordEvent = new TimedEvent();
            skippingRecord = false;

            // if the idName starts with @, it's an attribute on recordName
            if (idName.startsWith("@")) {
                // handle attributes as idName
                if (xpp.getAttributeCount() < 1) {
                    throw new XmlPullParserException(
                            "found no attributes for recordName = "
                                    + recordName + ", idName=" + idName
                                    + " at " + xpp.getPositionDescription());
                }
                // try with and without a namespace: first, try without
                String id = xpp.getAttributeValue("", idName.substring(1));
                if (id == null) {
                    id = xpp.getAttributeValue(recordNamespace, idName
                            .substring(1));
                }
                logger.fine("found id " + idName + " = " + id);
                // always set the uri, since we use it as a flag too
                uri = composeUri(id);

                if (checkStartId(id)) {
                    return;
                }
                if (checkExistingUri(uri)) {
                    skippingRecord = true;
                    return;
                }

                // open the docInsertStream for this attribute-id document
                current = openDocument(id);
                // let the fall-through code handle the rest
            } else {
                // we don't know the URI yet, so we can't open the stream yet
                // so we'll buffer up the contents until we do...
                // note that we might simply throw this away, too
                currentPrepend = new StringBuffer();
            }
        }

        // allow for repeated idName elements: use the first one we see, for
        // each recordName
        String text = xpp.getText();
        if (current == null && currentPrepend != null && uri == null
                && name.equals(idName)) {
            // pick out the contents and use it for the uri
            if (xpp.next() != XmlPullParser.TEXT)
                throw new XmlPullParserException(
                        "badly formed xml: missing id at "
                                + xpp.getPositionDescription());
            String id = xpp.getText();

            // always set the uri, since we use it as a flag too
            uri = composeUri(id);

            if (checkStartId(id)) {
                return;
            }
            if (checkExistingUri(uri)) {
                skippingRecord = true;
                return;
            }

            // now we know that we'll use this content and id
            currentPrepend.append(text).append(id);

            // open the docInsertStream for this element-id document,
            // and handle the pre-buffered xml.
            current = openDocument(id);

            // advance xpp to the END_ELEMENT - brittle?
            if (xpp.next() != XmlPullParser.END_TAG)
                throw new XmlPullParserException(
                        "badly formed xml: no END_TAG after id text"
                                + xpp.getPositionDescription());
            // logger.finest("END_TAG = " + xpp.getText());
            text = xpp.getText();

            // write it all out
            currentPrepend.append(text);
            current.write(currentPrepend.toString().getBytes());
            recordEvent.increment(currentPrepend.length());
            currentPrepend = null;
            return;
        }

        // if the startId is still defined, and the uri has been found,
        // we should skip as much of this work as possible
        // this avoids OutOfMemory too
        if (startId != null && uri != null) {
            return;
        }

        // ok, we seem to be inside a record
        // check to make sure!
        if (current == null && currentPrepend == null) {
            // silently skip element in a skipped record
            if (skippingRecord) {
                return;
            }
            if (ignoreUnknown) {
                logger.warning("skipping unknown non-record element: "
                        + xpp.getName());
                return;
            }
            throw new XmlPullParserException("unknown non-record element: "
                    + xpp.getName());
        }

        // this seems to be the only way to handle empty elements:
        // write it as a end-element, only
        // TODO what happens to attributes in this case?
        if (xpp.isEmptyElementTag())
            return;

        // that's it for the special-cases...
        // write it to current or currentPrepend
        if (current == null) {
            // we'll do the recordEvent accounting when we finish the prepend
            currentPrepend.append(text);
        } else {
            current.write(text.getBytes());
            recordEvent.increment(text.length());
        }
    }

    private XDMPDocInsertStream openDocument(String _id) throws XDBCException {
        // docOptions have already been initialized
        // handle collectionsMap, if present
        if (collectionMap != null) {
            // in this case we have to reset the whole collection list every
            // time, to prevent any carryover from the previous call to
            // docOptions.setCollections().
            List collections = new ArrayList(Arrays.asList(baseCollections));
            if (currentFileBasename != null) {
                collections.add(currentFileBasename);
            }
            if (collectionMap.containsKey(_id)) {
                // each map entry is a String[]
                collections.addAll(Arrays.asList((String[]) collectionMap
                        .get(_id)));
            }
            docOpts.setCollections((String[]) collections
                    .toArray(new String[0]));
        }
        logger.fine("uri: " + uri);
        logger.fine("docOpts: " + docOpts);
        return conn.openDocInsertStream(uri, docOpts);
    }

    /**
     * @param uri
     * @return
     * @throws XDBCException
     */
    private boolean checkExistingUri(String uri) throws XDBCException {
        // return true if we're supposed to check,
        // and if the document already exists
        if (skipExisting || errorExisting) {
            boolean exists = conn.checkFile(uri);
            logger.fine("checking for uri " + uri + " = " + exists);
            if (exists) {
                if (errorExisting) {
                    throw new XDBCException(
                            "ERROR_EXISTING=true, cannot overwrite existing document: "
                                    + uri);
                }
                // ok, must be skipExisting...
                logger.info("skipping " + (++totalSkipped) + " existing uri "
                        + uri);
                return true;
            }
        }
        return false;
    }

    private String composeUri(String id) {
        // logger.finer("uri = " + uri);
        if (currentFileBasename == null || currentFileBasename.equals("")) {
            return uriPrefix + id + uriSuffix;
        } else {
            // automatically use the current file too
            return uriPrefix + currentFileBasename + "/" + id + uriSuffix;
        }
    }

    private void processEndElement() throws IOException,
            XmlPullParserException, TimerEventException, XDBCException {
        // ignore if no current element has been set
        if (current == null && currentPrepend == null) {
            logger.finest("skipping end element: no current record");
            return;
        }

        // we should never get this far unless recordName has been set
        if (recordName == null) {
            throw new XmlPullParserException(
                    "end of element before recordName was set");
        }

        // record the element text
        String text = xpp.getText();
        logger.finest(text);

        if (current == null) {
            currentPrepend.append(text);
        } else {
            current.write(text.getBytes());
            recordEvent.increment(text.length());
        }

        if (!(recordName.equals(xpp.getName()) && recordNamespace.equals(xpp
                .getNamespace()))) {
            // not the end of the record: go look for more nodes
            return;
        }

        // end of record: were we skipping?
        if (skippingRecord) {
            logger.fine("reached the end of skipped record");
            closeRecord();
            return;
        }

        // finish the database document, if appropriate
        if (startId != null) {
            if (uri != null) {
                logger.fine("ignoring end of record for " + uri + ": START_ID "
                        + startId + " not yet found");
            }
            if (current != null) {
                current.abort();
            }
            closeRecord();
            return;
        }
        if (current == null) {
            logger.fine("ignoring end of record " + recordName + " for " + uri
                    + ": START_ID " + startId + " not yet found");
        }
        current.flush();

        boolean retry = true;
        while (retry) {
            try {
                current.commit();
                retry = false;
            } catch (XDBCXQueryException e) {
                e.printStackTrace();
                logger.logException(e.getMessage(), e);
                if (!e.getRetryable()) {
                    logger.warning("non-retryable exception!");
                    retry = false;
                    throw e;
                }
                logger.warning("sleeping before retry");
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e1) {
                    logger.logException("interrupted while sleeping in retry",
                            e1);
                }
            }
        }

        // TODO bottleneck? method is synchronized
        timer.add(recordEvent);
        if (System.currentTimeMillis() - lastDisplayMillis > DISPLAY_MILLIS) {
            lastDisplayMillis = System.currentTimeMillis();
            displayProgress();
        }
        // done: clean up for the next record
        closeRecord();
    }

    private void closeRecord() {
        if (current != null) {
            try {
                current.close();
            } catch (IOException e) {
            }
        }
        current = null;
        currentPrepend = null;
        uri = null;
        recordEvent = null;
        skippingRecord = false;
    }

    private void displayProgress() {
        logger.info("inserted record " + timer.getEventCount() + " as " + uri
                + " (" + timer.getDuration() + " ms, " + timer.getBytes()
                + " B, " + Math.round(timer.getEventRate()) + " tps, "
                + Math.round(timer.getThroughput()) + " kB/s" + ")");
    }

    private void processText() throws XmlPullParserException, IOException {
        if (currentPrepend == null && current == null)
            return;

        // if the startId is still defined, and the uri has been found,
        // we should skip as much of this work as possible
        // this avoids OutOfMemory too
        if (startId != null && uri != null) {
            return;
        }

        String text = xpp.getText();

        if (xpp.getEventType() == XmlPullParser.TEXT)
            text = Utilities.escapeXml(text);

        // logger.finest("processText = " + text);
        if (current == null) {
            currentPrepend.append(text);
        } else {
            current.write(text.getBytes());
        }
        recordEvent.increment(text.length());
    }

    public void run() {
        try {
            process();
        } catch (Exception e) {
            logger.logException("fatal error", e);
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