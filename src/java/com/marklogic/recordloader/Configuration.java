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
import java.io.InputStream;
import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.CodingErrorAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlPullParserFactory;

import com.marklogic.ps.RecordLoader;
import com.marklogic.ps.SimpleLogger;
import com.marklogic.ps.Utilities;
import com.marklogic.xcc.ContentPermission;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.ContentSourceFactory;
import com.marklogic.xcc.ContentbaseMetaData;
import com.marklogic.xcc.DocumentRepairLevel;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.XccException;

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
    private static final String INPUT_PATTERN_DEFAULT = "^.+\\.xml$";

    /**
     * 
     */
    private static final String CONNECTION_STRING_DEFAULT = "xcc://admin:admin@localhost:9000/";

    private static SimpleLogger logger = null;

    /**
     * 
     */
    static final String FATAL_ERRORS_KEY = "FATAL_ERRORS";

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
    static final String DEFAULT_NAMESPACE_KEY = "DEFAULT_NAMESPACE";

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
    static public final int DISPLAY_MILLIS = 3000;

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
    static final String THREADS_KEY = "THREADS";

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
    static final String UNRESOLVED_ENTITY_POLICY_KEY = "UNRESOLVED_ENTITY_POLICY";

    /**
     * 
     */
    static final String UNRESOLVED_ENTITY_POLICY_IGNORE = "IGNORE";

    /**
     * 
     */
    static final String UNRESOLVED_ENTITY_POLICY_REPLACE = "REPLACE";

    /**
     * 
     */
    static final String UNRESOLVED_ENTITY_POLICY_REPORT = "REPORT";

    /**
     * 
     */
    static final String UNRESOLVED_ENTITY_POLICY_DEFAULT = UNRESOLVED_ENTITY_POLICY_REPORT;

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
    static final String OUTPUT_FORESTS_KEY = "OUTPUT_FORESTS";

    /**
     * 
     */
    static final String OUTPUT_READ_ROLES_KEY = "READ_ROLES";

    /**
     * 
     */
    static final String OUTPUT_ENCODING_DEFAULT = "UTF-8";

    private static final String ZIP_INPUT_PATTERN_KEY = "ZIP_INPUT_PATTERN";

    private static final String ZIP_INPUT_PATTERN_DEFAULT = null;

    private Properties props = new Properties();

    private String[] baseCollections;

    private URI[] uris;

    private String entityPolicy = UNRESOLVED_ENTITY_POLICY_DEFAULT;

    private boolean errorExisting = false;

    private XmlPullParserFactory factory = null;

    private boolean fatalErrors = true;

    private boolean ignoreUnknown;

    private String idNodeName;

    private String inputEncoding;

    private String inputPath;

    private String inputPattern;

    private String malformedInputAction;

    private String recordName;

    private String recordNamespace;

    private DocumentRepairLevel repairLevel = DocumentRepairLevel.NONE;

    private boolean skipExisting = false;

    private String startId = null;

    private int threadCount;

    private String uriPrefix = OUTPUT_URI_SUFFIX_DEFAULT;

    private String uriSuffix = OUTPUT_URI_SUFFIX_DEFAULT;

    private boolean useAutomaticIds = false;

    private boolean useFileNameIds = false;

    private BigInteger[] placeKeys;

    private Object placeKeysMutex = new Object();

    private Object autoIdMutex = new Object();

    private int autoid = 1;

    private String zipInputPattern;

    /**
     * @param _props
     */
    public void load(Properties _props) {
        props.putAll(_props);
    }

    /**
     * @throws IOException
     * @throws URISyntaxException
     * 
     */
    public void configure() throws IOException, URISyntaxException {
        logger.configureLogger(props);

        idNodeName = props.getProperty(ID_NAME_KEY);
        if (idNodeName == null) {
            throw new IOException("missing required property: "
                    + ID_NAME_KEY);
        }

        // some or all of these may be null
        configureOptions();

        configureCollections();

        String[] connectionStrings = props.getProperty(
                CONNECTION_STRING_KEY, CONNECTION_STRING_DEFAULT).split(
                "\\s+");
        logger.info("connecting to "
                + Utilities.join(connectionStrings, " "));
        uris = new URI[connectionStrings.length];
        for (int i = 0; i < uris.length; i++) {
            uris[i] = new URI(connectionStrings[i]);
        }
    }

    private void configureOptions() {
        recordName = props.getProperty(RECORD_NAME_KEY);
        recordNamespace = props.getProperty(RECORD_NAMESPACE_KEY);
        if (recordName != null && recordNamespace == null)
            recordNamespace = OUTPUT_URI_SUFFIX_DEFAULT;

        ignoreUnknown = Utilities.stringToBoolean(props.getProperty(
                IGNORE_UNKNOWN_KEY, "false"));

        // use prefix to set document-uri patterns
        uriPrefix = props.getProperty(OUTPUT_URI_PREFIX_KEY,
                OUTPUT_URI_PREFIX_DEFAULT);
        if (!uriPrefix.equals(OUTPUT_URI_PREFIX_DEFAULT)
                && !uriPrefix.endsWith("/")) {
            uriPrefix += "/";
        }
        logger.fine(OUTPUT_URI_PREFIX_KEY + " = " + uriPrefix);

        uriSuffix = props.getProperty(OUTPUT_URI_SUFFIX_KEY,
                OUTPUT_URI_SUFFIX_DEFAULT);
        logger.fine(OUTPUT_URI_SUFFIX_KEY + " = " + uriSuffix);

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

        if (idNodeName.equals(ID_NAME_AUTO)) {
            logger.info("generating automatic ids");
            useAutomaticIds = true;
        }

        if (idNodeName.equals(ID_NAME_FILENAME)) {
            logger.info("generating ids from file names");
            useFileNameIds = true;
        }

        String repairString = props.getProperty(REPAIR_LEVEL_KEY,
                OUTPUT_URI_SUFFIX_DEFAULT + "NONE");
        if (repairString.equals("FULL")) {
            logger.fine(REPAIR_LEVEL_KEY + "=FULL");
            repairLevel = DocumentRepairLevel.FULL;
        }

        fatalErrors = Utilities.stringToBoolean(props.getProperty(
                FATAL_ERRORS_KEY, FATAL_ERRORS_DEFAULT));

        entityPolicy = props.getProperty(UNRESOLVED_ENTITY_POLICY_KEY,
                UNRESOLVED_ENTITY_POLICY_DEFAULT);

        inputEncoding = props.getProperty(INPUT_ENCODING_KEY,
                OUTPUT_ENCODING_DEFAULT);
        malformedInputAction = props.getProperty(
                INPUT_MALFORMED_ACTION_KEY,
                INPUT_MALFORMED_ACTION_DEFAULT);
        logger.info("using output encoding " + OUTPUT_ENCODING_DEFAULT);

        threadCount = Integer.parseInt(props
                .getProperty(THREADS_KEY, "1"));
        inputPath = props.getProperty(INPUT_PATH_KEY);
        logger.fine(INPUT_PATH_KEY + " = " + inputPath);
        inputPattern = props.getProperty(INPUT_PATTERN_KEY,
                INPUT_PATTERN_DEFAULT);
        logger.fine(INPUT_PATTERN_KEY + " = " + inputPattern);

        zipInputPattern = props.getProperty(ZIP_INPUT_PATTERN_KEY,
                ZIP_INPUT_PATTERN_DEFAULT);
        logger.fine(ZIP_INPUT_PATTERN_KEY + " = " + zipInputPattern);
    }

    private void configureCollections() {
        // initialize collections
        List<String> collections = new ArrayList<String>();
        collections.add(RecordLoader.NAME + "."
                + System.currentTimeMillis());
        logger.info("adding extra collection: " + collections.get(0));
        String collectionsString = props
                .getProperty(OUTPUT_COLLECTIONS_KEY);
        if (collectionsString != null
                && !collectionsString.equals(OUTPUT_URI_SUFFIX_DEFAULT)) {
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

    public String getEntityPolicy() {
        return entityPolicy;
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

    /**
     * @return
     */
    public boolean isUnresolvedEntityIgnore() {
        return UNRESOLVED_ENTITY_POLICY_IGNORE.equals(entityPolicy);
    }

    /**
     * @return
     */
    public boolean isUnresolvedEntityReplace() {
        return UNRESOLVED_ENTITY_POLICY_REPLACE.equals(entityPolicy);
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
        props.load(_stream);
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
     * @return
     */
    public ContentPermission[] getPermissions() {
        ContentPermission[] permissions = null;
        String readRolesString = props.getProperty(OUTPUT_READ_ROLES_KEY,
                OUTPUT_URI_SUFFIX_DEFAULT);
        if (readRolesString != null && readRolesString.length() > 0) {
            String[] readRoles = readRolesString.trim().split("\\s+");
            if (readRoles != null && readRoles.length > 0) {
                permissions = new ContentPermission[readRoles.length];
                for (int i = 0; i < readRoles.length; i++) {
                    if (readRoles[i] != null
                            && !readRoles[i]
                                    .equals(OUTPUT_URI_SUFFIX_DEFAULT))
                        permissions[i] = new ContentPermission(
                                ContentPermission.READ, readRoles[i]);
                }
            }
        }
        return permissions;
    }

    /**
     * @return
     */
    public String getOutputNamespace() {
        return props.getProperty(DEFAULT_NAMESPACE_KEY);
    }

    /**
     * @return
     * @throws XccException
     */
    public BigInteger[] getPlaceKeys() throws XccException {
        // lazy initialization
        if (placeKeys == null) {
            synchronized (placeKeysMutex) {
                String forestNames = props
                        .getProperty(OUTPUT_FORESTS_KEY);
                if (forestNames != null) {
                    forestNames = forestNames.trim();
                    if (!forestNames.equals(OUTPUT_URI_SUFFIX_DEFAULT)) {
                        logger.info("sending output to forests: "
                                + forestNames);
                        logger.fine("querying for Forest ids");
                        String[] placeNames = forestNames.split("\\s+");
                        ContentSource cs = ContentSourceFactory
                                .newContentSource(getConnectionStrings()[0]);
                        // be sure to use the default db
                        Session session = cs.newSession();
                        ContentbaseMetaData meta = session
                                .getContentbaseMetaData();
                        Map forestMap = meta.getForestMap();
                        for (int i = 0; i < placeNames.length; i++) {
                            placeKeys[i] = (BigInteger) forestMap
                                    .get(placeNames[i]);
                            logger.fine("mapping " + placeNames[i]
                                    + " to " + placeKeys[i]);
                        }
                    }
                }
            }
        }
        return placeKeys;
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
            factory = XmlPullParserFactory.newInstance(props
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
            return OUTPUT_URI_SUFFIX_DEFAULT + (autoid++);
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

}
