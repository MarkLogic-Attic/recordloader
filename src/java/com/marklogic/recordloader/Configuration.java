/**
 * Copyright (c) 2006-2009 Mark Logic Corporation. All rights reserved.
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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlPullParserFactory;

import com.marklogic.ps.RecordLoader;
import com.marklogic.ps.Utilities;
import com.marklogic.recordloader.xcc.XccConfiguration;
import com.marklogic.recordloader.xcc.XccContentFactory;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class Configuration extends AbstractConfiguration {

    /**
    *
    */
    public static final String CONNECTION_STRING_KEY = "CONNECTION_STRING";

    /**
     *
     */
    public static final String FATAL_ERRORS_DEFAULT = "true";

    /**
     *
     */
    public static final String INPUT_PATTERN_DEFAULT = "^.+\\.[Xx][Mm][Ll]$";

    /**
     *
     */
    public static final String FATAL_ERRORS_KEY = "FATAL_ERRORS";

    /**
    *
    */
    public static final String DELETE_INPUT_FILES_KEY = "DELETE_INPUT_FILES";

    /**
     *
     */
    public static final String DOCUMENT_FORMAT_KEY = "DOCUMENT_FORMAT";

    /**
    *
    */
    public static final String ERROR_EXISTING_KEY = "ERROR_EXISTING";

    public static final String ERROR_EXISTING_DEFAULT = "false";

    /**
     *
     */
    public static final String INPUT_PATTERN_KEY = "INPUT_PATTERN";

    /**
     *
     */
    public static final String INPUT_FILE_SIZE_LIMIT_KEY = "INPUT_FILE_SIZE_LIMIT";

    /**
     *
     */
    public static final String INPUT_FILE_SIZE_LIMIT_DEFAULT = "0";

    /**
     *
     */
    public static final String INPUT_PATH_KEY = "INPUT_PATH";

    /**
    *
    */
    public static final String INPUT_STREAMING_KEY = "INPUT_STREAMING";

    public static final String INPUT_STREAMING_DEFAULT = "false";

    /**
     *
     */
    public static final String INPUT_STRIP_PREFIX_KEY = "INPUT_STRIP_PREFIX";

    /**
     *
     */
    public static final String INPUT_NORMALIZE_PATHS_KEY = "INPUT_NORMALIZE_PATHS";

    /**
     *
     */
    public static final String INPUT_NORMALIZE_PATHS_DEFAULT = "false";

    /**
     *
     */
    public static final String OUTPUT_NAMESPACE_KEY = "DEFAULT_NAMESPACE";

    public static final String OUTPUT_NAMESPACE_DEFAULT = "";

    public static final String ID_NAME_KEY = "ID_NAME";

    public static final String ID_NAME_AUTO = "#AUTO";

    public static final String ID_NAME_FILENAME = "#FILENAME";

    public static final String ID_NAME_DEFAULT = ID_NAME_FILENAME;

    /**
     *
     */
    public static final String IGNORE_UNKNOWN_KEY = "IGNORE_UNKNOWN";

    /**
     *
     */
    public static final int DISPLAY_MILLIS = 15000;

    /**
     *
     */
    public static final String INPUT_ENCODING_KEY = "INPUT_ENCODING";

    public static final String INPUT_ESCAPE_IDS_KEY = "INPUT_ESCAPE_IDS";

    public static final String INPUT_ESCAPE_IDS_DEFAULT = "false";

    /**
     *
     */
    public static final String INPUT_MALFORMED_ACTION_KEY = "INPUT_MALFORMED_ACTION";

    /**
     *
     */
    public static final String INPUT_MALFORMED_ACTION_IGNORE = CodingErrorAction.IGNORE
            .toString();

    /**
     *
     */
    public static final String INPUT_MALFORMED_ACTION_REPLACE = CodingErrorAction.REPLACE
            .toString();

    /**
     *
     */
    public static final String INPUT_MALFORMED_ACTION_REPORT = CodingErrorAction.REPORT
            .toString();

    /**
     *
     */
    public static final String INPUT_MALFORMED_ACTION_DEFAULT = INPUT_MALFORMED_ACTION_REPORT;

    public static final String RECORD_NAME_DOCUMENT_ROOT = "#DOCUMENT";

    /**
     *
     */
    public static final String UNRESOLVED_ENTITY_REPLACEMENT_PREFIX = "<!-- UNRESOLVED-ENTITY ";

    /**
     *
     */
    public static final String UNRESOLVED_ENTITY_REPLACEMENT_SUFFIX = " -->";

    /**
     *
     */
    public static final String SKIP_EXISTING_KEY = "SKIP_EXISTING";

    public static final String SKIP_EXISTING_DEFAULT = "false";

    public static final String SKIP_EXISTING_UNTIL_FIRST_MISS_KEY = "SKIP_EXISTING_UNTIL_FIRST_MISS";

    public static final String SKIP_EXISTING_UNTIL_FIRST_MISS_DEFAULT = "false";

    /**
     *
     */
    public static final String START_ID_KEY = "START_ID";

    public static final String START_ID_MULTITHREADED_KEY = "START_ID_MULTITHREADED";

    public static final String START_ID_MULTITHREADED_DEFAULT = "false";

    /**
     *
     */
    public static final String THREADS_KEY = "THREADS";

    /**
    *
    */
    public static final String THREADS_DEFAULT = "1";

    public static final String THROTTLE_EVENTS_KEY = "THROTTLE_EVENTS_PER_SECOND";

    public static final String THROTTLE_EVENTS_DEFAULT = "0";

    public static final String THROTTLE_BYTES_KEY = "THROTTLE_BYTES_PER_SECOND";

    public static final String THROTTLE_BYTES_DEFAULT = "0";

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
    public static final int SLEEP_TIME = 500;

    /**
     *
     */
    public static final String REPAIR_LEVEL_KEY = "XML_REPAIR_LEVEL";

    public static final String REPAIR_LEVEL_DEFAULT = "NONE";

    /**
     *
     */
    public static final String OUTPUT_URI_SUFFIX_KEY = "URI_SUFFIX";

    /**
     *
     */
    public static final String OUTPUT_URI_PREFIX_KEY = "URI_PREFIX";

    /**
     *
     */
    public static final String OUTPUT_URI_PREFIX_DEFAULT = "";

    /**
     *
     */
    public static final String OUTPUT_URI_SUFFIX_DEFAULT = "";

    /**
     *
     */
    public static final String OUTPUT_COLLECTIONS_KEY = "OUTPUT_COLLECTIONS";

    public static final String OUTPUT_ENCODING_KEY = "OUTPUT_ENCODING";

    public static final String OUTPUT_ENCODING_DEFAULT = "UTF-8";

    public static final String COPY_NAMESPACES_KEY = "COPY_NAMESPACES";

    public static final String COPY_NAMESPACES_DEFAULT = "true";

    public static final String RECORD_NAMESPACE_DEFAULT = "";

    public static final int DEFAULT_CAPACITY = 1000;

    public static final String CONTENT_FACTORY_CLASSNAME_KEY = "CONTENT_FACTORY_CLASSNAME";

    public static final String CONTENT_FACTORY_CLASSNAME_DEFAULT = XccContentFactory.class
            .getCanonicalName();

    public static final String CONFIGURATION_CLASSNAME_KEY = "CONFIGURATION_CLASSNAME";

    public static final String CONFIGURATION_CLASSNAME_DEFAULT = XccConfiguration.class
            .getCanonicalName();

    public static final String LOADER_CLASSNAME_KEY = "LOADER_CLASSNAME";

    public static final String LOADER_CLASSNAME_DEFAULT = Loader.class
            .getName();

    public static final String LOOP_FOREVER_KEY = "LOOP_FOREVER";

    public static final String USE_FILENAME_COLLECTION_KEY = "USE_FILENAME_COLLECTION";

    public static final String USE_FILENAME_COLLECTION_DEFAULT = "true";

    public static final String QUEUE_CAPACITY_KEY = "QUEUE_CAPACITY";

    private String[] baseCollections;

    private URI[] uris;

    private XmlPullParserFactory factory = null;

    private boolean ignoreUnknown;

    private String idNodeName;

    private String inputEncoding;

    private String inputPath;

    private String inputStripPrefix;

    private boolean inputNormalizePaths;

    protected String inputPattern;

    private String malformedInputAction;

    private volatile String recordName;

    private volatile String recordNamespace;

    private String startId = null;

    private int threadCount;

    private String uriPrefix = OUTPUT_URI_PREFIX_DEFAULT;

    private String uriSuffix = OUTPUT_URI_SUFFIX_DEFAULT;

    private boolean useAutomaticIds = false;

    private boolean useFilenameIds = false;

    private AtomicInteger autoid = new AtomicInteger(1);

    private boolean copyNamespaceDeclarations = true;

    private int capacity = DEFAULT_CAPACITY;

    private double throttledEventsPerSecond;

    private int throttledBytesPerSecond;

    private volatile Constructor<? extends ContentFactory> contentFactoryConstructor;

    private Object contentFactoryMutex = new Object();

    private boolean useDocumentRoot = false;

    private boolean isFirstLoop = true;

    public static final String ZIP_SUFFIX = ".zip";

    public static final String INPUT_HANDLER_CLASSNAME_KEY = "INPUT_HANDLER_CLASSNAME";

    public static final String INPUT_HANDLER_CLASSNAME_DEFAULT = DefaultInputHandler.class
            .getCanonicalName();

    public static final String INPUT_ENCODING_DEFAULT = OUTPUT_ENCODING_DEFAULT;

    public static final String IGNORE_FILE_BASENAME_KEY = "IGNORE_FILE_BASENAME";

    public static final String IGNORE_FILE_BASENAME_DEFAULT = "false";

    /**
     * @throws URISyntaxException
     * 
     */
    public void configure() {
        // set up the logger early, for verbose configuration output
        logger.configureLogger(properties);

        try {
            setDefaults();
        } catch (Exception e) {
            // crude, but this is a configuration-time error
            throw new FatalException(e);
        }
        validateProperties();

        setIdNodeName(properties.getProperty(ID_NAME_KEY));

        // some or all of these may be null
        configureOptions();
        configureCollections();

        String[] connectionStrings = properties.getProperty(
                CONNECTION_STRING_KEY).split("[,\\s]+");
        logger.info("connecting to "
                + Utilities.join(connectionStrings, " "));
        uris = new URI[connectionStrings.length];
        try {
            for (int i = 0; i < uris.length; i++) {
                uris[i] = new URI(connectionStrings[i]);
            }
        } catch (URISyntaxException e) {
            throw new FatalException(e);
        }
    }

    private void configureOptions() {
        recordName = properties.getProperty(RECORD_NAME_KEY);
        if (RECORD_NAME_DOCUMENT_ROOT.equals(recordName)) {
            // whatever the document root is, we will use it
            logger.fine("using document root as record name");
            useDocumentRoot = true;
            recordName = null;
        }
        if (null != recordName) {
            recordNamespace = properties
                    .getProperty(RECORD_NAMESPACE_KEY);
        }

        ignoreUnknown = Utilities.stringToBoolean(properties
                .getProperty(IGNORE_UNKNOWN_KEY));

        // use prefix to set document-uri patterns
        uriPrefix = properties.getProperty(OUTPUT_URI_PREFIX_KEY);
        if (!OUTPUT_URI_PREFIX_DEFAULT.equals(uriPrefix)
                && !uriPrefix.endsWith("/")) {
            uriPrefix += "/";
        }

        uriSuffix = properties.getProperty(OUTPUT_URI_SUFFIX_KEY);

        // look for startId, to skip records
        startId = properties.getProperty(START_ID_KEY);

        // copy SKIP_EXISTING from SKIP_EXISTING_UNTIL_FIRST_MISS, if true
        if (isSkipExistingUntilFirstMiss()) {
            setSkipExisting(true);
        }

        copyNamespaceDeclarations = Utilities.stringToBoolean(properties
                .getProperty(COPY_NAMESPACES_KEY));

        inputEncoding = properties.getProperty(INPUT_ENCODING_KEY);
        malformedInputAction = properties
                .getProperty(INPUT_MALFORMED_ACTION_KEY);
        logger.info("using input encoding " + inputEncoding);
        logger.info("using malformed input action "
                + malformedInputAction);

        threadCount = Integer.parseInt(properties
                .getProperty(THREADS_KEY));
        capacity = Integer.parseInt(properties.getProperty(
                QUEUE_CAPACITY_KEY, "" + DEFAULT_CAPACITY * threadCount));

        inputPath = properties.getProperty(INPUT_PATH_KEY);

        inputPattern = properties.getProperty(INPUT_PATTERN_KEY);
        inputStripPrefix = properties.getProperty(INPUT_STRIP_PREFIX_KEY);
        inputNormalizePaths = Utilities.stringToBoolean(properties
                .getProperty(INPUT_NORMALIZE_PATHS_KEY));

        configureThrottling();
    }

    /**
     * 
     */
    void configureThrottling() {
        // do not throttle while skipExistingUntilFirstMiss is active
        if (isSkipExistingUntilFirstMiss() && isSkipExisting()) {
            return;
        }
        throttledEventsPerSecond = Double.parseDouble(properties
                .getProperty(THROTTLE_EVENTS_KEY));

        throttledBytesPerSecond = Integer.parseInt(properties
                .getProperty(THROTTLE_BYTES_KEY));
    }

    private void configureCollections() {
        // initialize collections
        List<String> collections = new ArrayList<String>();
        collections.add(RecordLoader.NAME + "."
                + System.currentTimeMillis());
        logger.info("adding extra collection: " + collections.get(0));
        String collectionsString = properties
                .getProperty(OUTPUT_COLLECTIONS_KEY);
        if (collectionsString != null && !collectionsString.equals("")) {
            collections.addAll(Arrays.asList(collectionsString
                    .split("[\\s,]+")));
        }
        // keep a base list of collections that can be extended later
        baseCollections = collections.toArray(new String[0]);
    }

    public String getRecordName() {
        return recordName;
    }

    public synchronized void setRecordName(String recordName) {
        this.recordName = recordName;
    }

    public String getRecordNamespace() {
        return recordNamespace;
    }

    public synchronized void setRecordNamespace(String recordNamespace) {
        this.recordNamespace = recordNamespace;
    }

    /**
     * @return
     */
    public String[] getBaseCollections() {
        return baseCollections;
    }

    public String getStartId() {
        return startId;
    }

    public String getUriSuffix() {
        return uriSuffix;
    }

    public void setUriSuffix(String uriSuffix) {
        this.uriSuffix = uriSuffix;
    }

    public boolean isFatalErrors() {
        return Utilities.stringToBoolean(properties
                .getProperty(FATAL_ERRORS_KEY));
    }

    public boolean isIgnoreUnknown() {
        return ignoreUnknown;
    }

    public boolean isUseAutomaticIds() {
        return useAutomaticIds;
    }

    public boolean isErrorExisting() {
        return Utilities.stringToBoolean(properties
                .getProperty(ERROR_EXISTING_KEY));
    }

    public boolean isSkipExisting() {
        return Utilities.stringToBoolean(properties
                .getProperty(SKIP_EXISTING_KEY));
    }

    public String getUriPrefix() {
        return uriPrefix;
    }

    public String getMalformedInputAction() {
        return malformedInputAction;
    }

    public String getInputEncoding() {
        return inputEncoding;
    }

    public String getInputPath() {
        return inputPath;
    }

    public String getInputPattern() {
        return inputPattern;
    }

    public int getThreadCount() {
        return threadCount;
    }

    public URI[] getConnectionStrings() {
        return uris;
    }

    public String getIdNodeName() {
        return idNodeName;
    }

    /**
     * 
     * @throws XmlPullParserException
     * @return
     */
    public XmlPullParserFactory getXppFactory()
            throws XmlPullParserException {
        // get a new factory
        if (factory == null) {
            factory = XmlPullParserFactory.newInstance(properties
                    .getProperty(XmlPullParserFactory.PROPERTY_NAME),
                    null);
            factory.setNamespaceAware(true);
        }
        return factory;
    }

    /**
     * @return
     */
    public boolean hasStartId() {
        return null != startId;
    }

    /**
     * @param _id
     */
    public void setStartId(String _id) {
        logger.finest("setting startId = " + _id);
        startId = _id;
    }

    /**
     * @return
     */
    public String getAutoId() {
        return "" + autoid.incrementAndGet();
    }

    public boolean isUseFilenameIds() {
        return useFilenameIds;
    }

    /**
     * @return
     */
    public boolean isCopyNamespaceDeclarations() {
        return copyNamespaceDeclarations;
    }

    /**
     * @param _name
     */
    public void setIdNodeName(String _name) {
        idNodeName = _name;

        if (null == _name) {
            logger.warning("no " + ID_NAME_KEY + " specified: using "
                    + ID_NAME_FILENAME);
            // just in case...
            properties.setProperty(ID_NAME_KEY, ID_NAME_FILENAME);
            idNodeName = ID_NAME_FILENAME;
        }

        logger.fine(ID_NAME_KEY + "=" + idNodeName);

        if (ID_NAME_AUTO.equals(idNodeName)) {
            setUseAutomaticIds();
        } else if (ID_NAME_FILENAME.equals(idNodeName)) {
            setUseFilenameIds();
        }
    }

    /**
     *
     */
    private void setUseFilenameIds() {
        logger.info("generating ids from file names");
        useAutomaticIds = false;
        useFilenameIds = true;
        properties.setProperty(LOADER_CLASSNAME_KEY, FileLoader.class
                .getName());
        // better to escape ids by default
        defaults.put(INPUT_ESCAPE_IDS_KEY, "true");
    }

    /**
     *
     */
    public void setUseAutomaticIds() {
        logger.info("generating automatic ids");
        useAutomaticIds = true;
        useFilenameIds = false;
    }

    /**
     * @return
     */
    public int getQueueCapacity() {
        return capacity;
    }

    /**
     * @return
     */
    public long getKeepAliveSeconds() {
        return 16;
    }

    /**
     * @return
     */
    public boolean isThrottled() {
        return (throttledEventsPerSecond > 0 || throttledBytesPerSecond > 0);
    }

    public double getThrottledEventsPerSecond() {
        return throttledEventsPerSecond;
    }

    public String getInputStripPrefix() {
        return inputStripPrefix;
    }

    public boolean isInputNormalizePaths() {
        return inputNormalizePaths;
    }

    /**
     * @return
     */
    public String getContentFactoryClassName() {
        return properties.getProperty(CONTENT_FACTORY_CLASSNAME_KEY);
    }

    /**
     * @return
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws IllegalArgumentException
     * @throws LoaderException
     */
    public Constructor<? extends ContentFactory> getContentFactoryConstructor()
            throws IllegalArgumentException, InstantiationException,
            IllegalAccessException, InvocationTargetException,
            LoaderException {
        if (null != contentFactoryConstructor) {
            return contentFactoryConstructor;
        }

        // singleton constructor - thread-safe?
        synchronized (contentFactoryMutex) {
            // check again, to avoid any race for the mutex
            if (null == contentFactoryConstructor) {
                // first time
                String className = getContentFactoryClassName();
                logger.info("ContentFactory is " + className);
                try {
                    Class<? extends ContentFactory> contentFactoryClass = Class
                            .forName(className, true,
                                    ClassLoader.getSystemClassLoader())
                            .asSubclass(ContentFactory.class);
                    contentFactoryConstructor = contentFactoryClass
                            .getConstructor(new Class[] {});
                } catch (Exception e) {
                    throw new FatalException("Bad "
                            + Configuration.CONTENT_FACTORY_CLASSNAME_KEY
                            + ": " + className, e);
                }
                // log version info
                ContentFactory cf = contentFactoryConstructor
                        .newInstance(new Object[] {});
                cf.setConfiguration(this);
                logger.info(cf.getVersionString());
            }
        }
        return contentFactoryConstructor;
    }

    /**
     * @return
     */
    public String getConfigurationClassName() {
        // keep the default - used to construct the pre-defaults configuration
        return properties.getProperty(CONFIGURATION_CLASSNAME_KEY,
                CONFIGURATION_CLASSNAME_DEFAULT);
    }

    /**
     * @return
     */
    public String getLoaderClassName() {
        return properties.getProperty(LOADER_CLASSNAME_KEY);
    }

    /**
     * @return
     */
    public boolean isUseFilenameCollection() {
        // When using filename ids, never add filename collections.
        // Otherwise, honor the property.
        // If true, we will add a collection for each input file's basename.
        // This is useful for record-set files, aka superfiles, but not for
        // record-files.
        if (isUseFilenameIds()) {
            return false;
        }
        return Utilities.stringToBoolean(properties
                .getProperty(USE_FILENAME_COLLECTION_KEY));
    }

    /**
     * @return
     */
    public boolean isUseDocumentRoot() {
        return useDocumentRoot;
    }

    /**
     * @return
     */
    public String getInputHandlerClassName() {
        return properties.getProperty(INPUT_HANDLER_CLASSNAME_KEY);
    }

    public CharsetDecoder getDecoder() {
        String inputEncoding = getInputEncoding();
        String malformedInputAction = getMalformedInputAction();

        CharsetDecoder inputDecoder;
        // using an explicit decoder allows us to control the error reporting
        inputDecoder = Charset.forName(inputEncoding).newDecoder();
        if (malformedInputAction
                .equals(Configuration.INPUT_MALFORMED_ACTION_IGNORE)) {
            inputDecoder.onMalformedInput(CodingErrorAction.IGNORE);
        } else if (malformedInputAction
                .equals(Configuration.INPUT_MALFORMED_ACTION_REPLACE)) {
            inputDecoder.onMalformedInput(CodingErrorAction.REPLACE);
        } else {
            inputDecoder.onMalformedInput(CodingErrorAction.REPORT);
        }
        inputDecoder.onUnmappableCharacter(CodingErrorAction.REPORT);
        return inputDecoder;
    }

    /**
     * @return
     */
    public int getThrottledBytesPerSecond() {
        return throttledBytesPerSecond;
    }

    /**
     * @return
     */
    public boolean isIgnoreFileBasename() {
        return Utilities.stringToBoolean(properties
                .getProperty(IGNORE_FILE_BASENAME_KEY));
    }

    /**
     * @return
     */
    public long getFileSizeLimit() {
        return Long.parseLong(properties
                .getProperty(INPUT_FILE_SIZE_LIMIT_KEY));
    }

    public boolean isEscapeUri() {
        return Utilities.stringToBoolean(properties
                .getProperty(INPUT_ESCAPE_IDS_KEY));
    }

    /**
     * @return
     */
    public boolean isInputStreaming() {
        return Utilities.stringToBoolean(properties
                .getProperty(INPUT_STREAMING_KEY));
    }

    /**
     * @return
     */
    public boolean isSkipExistingUntilFirstMiss() {
        return Utilities.stringToBoolean(properties
                .getProperty(SKIP_EXISTING_UNTIL_FIRST_MISS_KEY));
    }

    /**
     * @param _value
     */
    public void setSkipExisting(boolean _value) {
        properties.setProperty(SKIP_EXISTING_KEY, "" + _value);
    }

    /**
     * @return
     */
    public boolean isStartIdMultiThreaded() {
        return Utilities.stringToBoolean(properties
                .getProperty(START_ID_MULTITHREADED_KEY));
    }

    /**
     * @return
     */
    public boolean isDeleteInputFile() {
        return Utilities.stringToBoolean(properties
                .getProperty(DELETE_INPUT_FILES_KEY));
    }

    /**
     * @return
     */
    public boolean isLoopForever() {
        return Utilities.stringToBoolean(properties
                .getProperty(LOOP_FOREVER_KEY));
    }

    /**
     * @return
     */
    public boolean isFirstLoop() {
        return isFirstLoop;
    }

    /**
     * @param _bool
     */
    public void setFirstLoop(boolean _bool) {
        isFirstLoop = _bool;
    }

}
