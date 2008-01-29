/**
 * Copyright (c) 2006-2008 Mark Logic Corporation. All rights reserved.
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
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.CodingErrorAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlPullParserFactory;

import com.marklogic.ps.RecordLoader;
import com.marklogic.ps.SimpleLogger;
import com.marklogic.ps.Utilities;
import com.marklogic.recordloader.xcc.XccConfiguration;
import com.marklogic.recordloader.xcc.XccContentFactory;
import com.marklogic.xcc.DocumentFormat;
import com.marklogic.xcc.DocumentRepairLevel;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class Configuration {

    /**
     * 
     */
    private static final String OUTPUT_URI_PREFIX_DEFAULT = "";

    /**
     * 
     */
    private static final String OUTPUT_URI_SUFFIX_DEFAULT = "";

    /**
     * 
     */
    private static final String FATAL_ERRORS_DEFAULT = "true";

    /**
     * 
     */
    private static final String INPUT_PATTERN_DEFAULT = "^.+\\.[Xx][Mm][Ll]$";

    /**
     * 
     */
    private static final String CONNECTION_STRING_DEFAULT = "xcc://admin:admin@localhost:9000/";

    protected static SimpleLogger logger = null;

    /**
     * 
     */
    static final String FATAL_ERRORS_KEY = "FATAL_ERRORS";

    /**
     * 
     */
    static final String DOCUMENT_FORMAT_KEY = "DOCUMENT_FORMAT";

    /**
     * 
     */
    private static final String DOCUMENT_FORMAT_DEFAULT = DocumentFormat.XML
            .toString();

    /**
     * 
     */
    static final String CONNECTION_STRING_KEY = "CONNECTION_STRING";

    /**
     * 
     */
    static final String INPUT_PATTERN_KEY = "INPUT_PATTERN";

    /**
     * 
     */
    static final String INPUT_PATH_KEY = "INPUT_PATH";

    /**
     * 
     */
    static final String INPUT_STRIP_PREFIX = "INPUT_STRIP_PREFIX";

    /**
     * 
     */
    static final String INPUT_NORMALIZE_PATHS = "INPUT_NORMALIZE_PATHS";

    /**
     * 
     */
    static final String INPUT_NORMALIZE_PATHS_DEFAULT = "false";

    /**
     * 
     */
    public static final String DEFAULT_NAMESPACE_KEY = "DEFAULT_NAMESPACE";

    /**
     * 
     */
    static final String ERROR_EXISTING_KEY = "ERROR_EXISTING";

    /**
     * 
     */
    static public final String ID_NAME_KEY = "ID_NAME";

    /**
     * 
     */
    static final String IGNORE_UNKNOWN_KEY = "IGNORE_UNKNOWN";

    /**
     * 
     */
    static public final int DISPLAY_MILLIS = 15000;

    /**
     * 
     */
    static public final String INPUT_ENCODING_KEY = "INPUT_ENCODING";

    /**
     * 
     */
    static public final String INPUT_MALFORMED_ACTION_KEY = "INPUT_MALFORMED_ACTION";

    /**
     * 
     */
    static public final String INPUT_MALFORMED_ACTION_IGNORE = CodingErrorAction.IGNORE
            .toString();

    /**
     * 
     */
    static public final String INPUT_MALFORMED_ACTION_REPLACE = CodingErrorAction.REPLACE
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

    /**
     * 
     */
    public static final String ID_NAME_AUTO = "#AUTO";

    public static final String ID_NAME_FILENAME = "#FILENAME";

    /**
     * 
     */
    static public final String UNRESOLVED_ENTITY_REPLACEMENT_PREFIX = "<!-- UNRESOLVED-ENTITY ";

    /**
     * 
     */
    static public final String UNRESOLVED_ENTITY_REPLACEMENT_SUFFIX = " -->";

    /**
     * 
     */
    static final String SKIP_EXISTING_KEY = "SKIP_EXISTING";

    /**
     * 
     */
    static final String START_ID_KEY = "START_ID";

    /**
     * 
     */
    public static final String THREADS_KEY = "THREADS";

    static final String THROTTLE_KEY = "THROTTLE_EVENTS_PER_SECOND";

    static final String THROTTLE_DEFAULT = "0";

    /**
     * 
     */
    static final String RECORD_NAMESPACE_KEY = "RECORD_NAMESPACE";

    /**
     * 
     */
    static final String RECORD_NAME_KEY = "RECORD_NAME";

    /**
     * 
     */
    static public final int SLEEP_TIME = 500;

    /**
     * 
     */
    static final String REPAIR_LEVEL_KEY = "XML_REPAIR_LEVEL";

    /**
     * 
     */
    static final String OUTPUT_URI_SUFFIX_KEY = "URI_SUFFIX";

    /**
     * 
     */
    static final String OUTPUT_URI_PREFIX_KEY = "URI_PREFIX";

    /**
     * 
     */
    static final String OUTPUT_COLLECTIONS_KEY = "OUTPUT_COLLECTIONS";

    /**
     * 
     */
    public static final String OUTPUT_READ_ROLES_KEY = "READ_ROLES";

    /**
     * 
     */
    static final String OUTPUT_ENCODING_DEFAULT = "UTF-8";

    private static final String ZIP_INPUT_PATTERN_KEY = "ZIP_INPUT_PATTERN";

    private static final String ZIP_INPUT_PATTERN_DEFAULT = null;

    private static final String COPY_NAMESPACES_KEY = "COPY_NAMESPACES";

    private static final String COPY_NAMESPACES_DEFAULT = "true";

    private static final String RECORD_NAMESPACE_DEFAULT = "";

    private static final int DEFAULT_CAPACITY = 1000;

    public static final String CONTENT_FACTORY_CLASSNAME_KEY = "CONTENT_FACTORY_CLASSNAME";

    public static final String CONTENT_FACTORY_CLASSNAME_DEFAULT = XccContentFactory.class
            .getCanonicalName();

    public static final String CONFIGURATION_CLASSNAME_KEY = "CONFIGURATION_CLASSNAME";

    public static final String CONFIGURATION_CLASSNAME_DEFAULT = XccConfiguration.class
            .getCanonicalName();

    protected Properties properties = new Properties();

    private String[] baseCollections;

    private URI[] uris;

    private boolean errorExisting = false;

    private XmlPullParserFactory factory = null;

    private boolean fatalErrors = true;

    private boolean ignoreUnknown;

    private String idNodeName;

    private String inputEncoding;

    private String inputPath;

    private String inputStripPrefix;

    private boolean inputNormalizePaths;

    private String inputPattern;

    private String malformedInputAction;

    private String recordName;

    private String recordNamespace;

    private DocumentRepairLevel repairLevel = DocumentRepairLevel.NONE;

    private boolean skipExisting = false;

    private String startId = null;

    private int threadCount;

    private String uriPrefix = OUTPUT_URI_PREFIX_DEFAULT;

    private String uriSuffix = OUTPUT_URI_SUFFIX_DEFAULT;

    private boolean useAutomaticIds = false;

    private boolean useFileNameIds = false;

    private Object autoIdMutex = new Object();

    private int autoid = 1;

    private String zipInputPattern;

    private boolean copyNamespaceDeclarations = true;

    private int capacity = DEFAULT_CAPACITY;

    private double throttledEventsPerSecond;

    private DocumentFormat format = DocumentFormat.XML;

    private Constructor<? extends ContentFactory> contentFactoryConstructor;

    private Object contentFactoryMutex = new Object();

    /**
     * @param _props
     */
    public void load(Properties _props) {
        properties.putAll(_props);
    }

    /**
     * @throws URISyntaxException
     * 
     */
    public void configure() throws URISyntaxException {
        logger.configureLogger(properties);

        setIdNodeName(properties.getProperty(ID_NAME_KEY));

        // some or all of these may be null
        configureOptions();
        configureCollections();

        String[] connectionStrings = properties.getProperty(
                CONNECTION_STRING_KEY, CONNECTION_STRING_DEFAULT).split(
                "[,\\s]+");
        logger.info("connecting to "
                + Utilities.join(connectionStrings, " "));
        uris = new URI[connectionStrings.length];
        for (int i = 0; i < uris.length; i++) {
            uris[i] = new URI(connectionStrings[i]);
        }
    }

    private void configureOptions() {
        recordName = properties.getProperty(RECORD_NAME_KEY);
        recordNamespace = properties.getProperty(RECORD_NAMESPACE_KEY);
        if (recordName != null && recordNamespace == null)
            recordNamespace = RECORD_NAMESPACE_DEFAULT;

        ignoreUnknown = Utilities.stringToBoolean(properties.getProperty(
                IGNORE_UNKNOWN_KEY, "false"));

        // use prefix to set document-uri patterns
        uriPrefix = properties.getProperty(OUTPUT_URI_PREFIX_KEY,
                OUTPUT_URI_PREFIX_DEFAULT);
        if (!uriPrefix.equals(OUTPUT_URI_PREFIX_DEFAULT)
                && !uriPrefix.endsWith("/")) {
            uriPrefix += "/";
        }
        logger.fine(OUTPUT_URI_PREFIX_KEY + " = " + uriPrefix);

        uriSuffix = properties.getProperty(OUTPUT_URI_SUFFIX_KEY,
                OUTPUT_URI_SUFFIX_DEFAULT);
        logger.fine(OUTPUT_URI_SUFFIX_KEY + " = " + uriSuffix);

        // look for startId, to skip records
        startId = properties.getProperty(START_ID_KEY);
        logger.fine("START_ID=" + startId);

        // should we check for existing docs?
        skipExisting = Utilities.stringToBoolean(properties.getProperty(
                SKIP_EXISTING_KEY, "false"));
        logger.fine("SKIP_EXISTING=" + skipExisting);

        // should we throw an error for existing docs?
        errorExisting = Utilities.stringToBoolean(properties.getProperty(
                ERROR_EXISTING_KEY, "false"));
        logger.fine("ERROR_EXISTING=" + errorExisting);

        String repairString = properties.getProperty(REPAIR_LEVEL_KEY,
                "NONE");
        if (repairString.equals("FULL")) {
            logger.fine(REPAIR_LEVEL_KEY + "=" + repairString);
            repairLevel = DocumentRepairLevel.FULL;
        }

        copyNamespaceDeclarations = Utilities
                .stringToBoolean(properties.getProperty(
                        COPY_NAMESPACES_KEY, COPY_NAMESPACES_DEFAULT));

        fatalErrors = Utilities.stringToBoolean(properties.getProperty(
                FATAL_ERRORS_KEY, FATAL_ERRORS_DEFAULT));

        inputEncoding = properties.getProperty(INPUT_ENCODING_KEY,
                OUTPUT_ENCODING_DEFAULT);
        malformedInputAction = properties.getProperty(
                INPUT_MALFORMED_ACTION_KEY,
                INPUT_MALFORMED_ACTION_DEFAULT);
        logger.info("using output encoding " + OUTPUT_ENCODING_DEFAULT);

        threadCount = Integer.parseInt(properties.getProperty(
                THREADS_KEY, "1"));
        capacity = DEFAULT_CAPACITY * threadCount;

        inputPath = properties.getProperty(INPUT_PATH_KEY);
        logger.fine(INPUT_PATH_KEY + " = " + inputPath);
        inputPattern = properties.getProperty(INPUT_PATTERN_KEY,
                INPUT_PATTERN_DEFAULT);
        inputStripPrefix = properties.getProperty(INPUT_STRIP_PREFIX);
        inputNormalizePaths = Utilities.stringToBoolean(properties
                .getProperty(INPUT_NORMALIZE_PATHS,
                        INPUT_NORMALIZE_PATHS_DEFAULT));
        logger.fine(INPUT_PATTERN_KEY + " = " + inputPattern);

        zipInputPattern = properties.getProperty(ZIP_INPUT_PATTERN_KEY,
                ZIP_INPUT_PATTERN_DEFAULT);
        logger.fine(ZIP_INPUT_PATTERN_KEY + " = " + zipInputPattern);

        throttledEventsPerSecond = Double.parseDouble(properties
                .getProperty(THROTTLE_KEY, THROTTLE_DEFAULT));
        if (isThrottled()) {
            logger.info("throttle = " + throttledEventsPerSecond);
        }

        String formatString = properties.getProperty(DOCUMENT_FORMAT_KEY,
                DOCUMENT_FORMAT_DEFAULT).toLowerCase();
        if (formatString.equals(DocumentFormat.TEXT.toString())) {
            format = DocumentFormat.TEXT;
        } else if (formatString.equals(DocumentFormat.BINARY.toString())) {
            format = DocumentFormat.BINARY;
        } else if (formatString.equals(DocumentFormat.XML.toString())) {
            format = DocumentFormat.XML;
        } else {
            logger.warning("Unexpected: " + DOCUMENT_FORMAT_KEY + "="
                    + formatString + " (using xml)");
            format = DocumentFormat.XML;
        }
        if (!format.equals(DocumentFormat.XML)) {
            logger.info("Using " + DOCUMENT_FORMAT_KEY + "="
                    + formatString);
        }
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

    /**
     * @param _logger
     */
    public void setLogger(SimpleLogger _logger) {
        logger = _logger;
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

    public DocumentRepairLevel getRepairLevel() {
        return repairLevel;
    }

    public String getUriSuffix() {
        return uriSuffix;
    }

    public void setUriSuffix(String uriSuffix) {
        this.uriSuffix = uriSuffix;
    }

    public boolean isFatalErrors() {
        return fatalErrors;
    }

    public boolean isIgnoreUnknown() {
        return ignoreUnknown;
    }

    /**
     * @return
     */
    public boolean isFullRepair() {
        return repairLevel == DocumentRepairLevel.FULL;
    }

    public boolean isUseAutomaticIds() {
        return useAutomaticIds;
    }

    public boolean isErrorExisting() {
        return errorExisting;
    }

    public boolean isSkipExisting() {
        return skipExisting;
    }

    public String getUriPrefix() {
        return uriPrefix;
    }

    /**
     * @param _stream
     * @throws IOException
     */
    public void load(InputStream _stream) throws IOException {
        properties.load(_stream);
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
     * @return
     */
    public SimpleLogger getLogger() {
        return logger;
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
        return startId != null;
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
        synchronized (autoIdMutex) {
            return "" + (autoid++);
        }
    }

    public boolean isUseFileNameIds() {
        return useFileNameIds;
    }

    /**
     * @return
     */
    public String getZipInputPattern() {
        return zipInputPattern;
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

        if (idNodeName.equals(ID_NAME_AUTO)) {
            logger.info("generating automatic ids");
            useAutomaticIds = true;
            useFileNameIds = false;
        } else if (idNodeName.equals(ID_NAME_FILENAME)) {
            logger.info("generating ids from file names");
            useAutomaticIds = false;
            useFileNameIds = true;
        }
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
    public boolean isFileBasedId() {
        return idNodeName.equals(ID_NAME_FILENAME);
    }

    /**
     * @return
     */
    public boolean isThrottled() {
        return throttledEventsPerSecond > 0;
    }

    public double getThrottledEventsPerSecond() {
        return throttledEventsPerSecond;
    }

    public DocumentFormat getFormat() {
        return format;
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
    public Properties getProperties() {
        return properties;
    }

    /**
     * @return
     */
    public String getContentFactoryClassName() {
        return properties.getProperty(CONTENT_FACTORY_CLASSNAME_KEY,
                CONTENT_FACTORY_CLASSNAME_DEFAULT);
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
        return properties.getProperty(CONFIGURATION_CLASSNAME_KEY,
                CONFIGURATION_CLASSNAME_DEFAULT);
    }

    /**
     * @return
     * @throws IllegalArgumentException
     * @throws ClassNotFoundException
     * @throws NoSuchMethodException
     * @throws SecurityException
     */
    public Constructor<? extends Configuration> getConfigurationConstructor()
            throws IllegalArgumentException, ClassNotFoundException,
            SecurityException, NoSuchMethodException {
        // this should only be called once, in a single-threaded main() context
        String className = getConfigurationClassName();
        logger.info("Configuration is " + className);
        Class<? extends Configuration> configurationClass = Class
                .forName(className, true,
                        ClassLoader.getSystemClassLoader()).asSubclass(
                        Configuration.class);
        Constructor<? extends Configuration> configurationConstructor = configurationClass
                .getConstructor(new Class[] {});
        return configurationConstructor;
    }
}
