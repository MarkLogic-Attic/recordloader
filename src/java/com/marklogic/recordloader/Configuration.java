/**
 * Copyright (c) 2006 Mark Logic Corporation. All rights reserved.
 */
package com.marklogic.recordloader;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.CodingErrorAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlPullParserFactory;

import com.marklogic.ps.Connection;
import com.marklogic.ps.RecordLoader;
import com.marklogic.ps.SimpleLogger;
import com.marklogic.ps.Utilities;
import com.marklogic.xdbc.XDBCException;
import com.marklogic.xdbc.XDBCResultSequence;
import com.marklogic.xdmp.XDMPDocInsertStream;
import com.marklogic.xdmp.XDMPDocOptions;
import com.marklogic.xdmp.XDMPPermission;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class Configuration {

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
    static final String INPUT_MALFORMED_ACTION_REPORT = CodingErrorAction.REPORT
            .toString();

    /**
     * 
     */
    static final String INPUT_MALFORMED_ACTION_DEFAULT = INPUT_MALFORMED_ACTION_REPORT;

    /**
     * 
     */
    static final Object ID_NAME_AUTO = "#AUTO";

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
    static final String DEFAULT_OUTPUT_ENCODING = "UTF-8";

    private Properties props = new Properties();

    private String[] baseCollections;

    private String[] connectionStrings;

    private String entityPolicy = Configuration.UNRESOLVED_ENTITY_POLICY_DEFAULT;

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

    private int repairLevel = XDMPDocInsertStream.XDMP_ERROR_CORRECTION_NONE;

    private boolean skipExisting = false;

    private String startId = null;

    private int threadCount;

    private String uriPrefix = "";

    private String uriSuffix = "";

    private boolean useAutomaticIds = false;

    /**
     * @param _props
     */
    public void load(Properties _props) {
        props.putAll(_props);
    }

    /**
     * @throws IOException
     * @throws XDBCException
     * 
     */
    public void configure() throws IOException, XDBCException {
        logger.configureLogger(props);

        idNodeName = props
                .getProperty(Configuration.ID_NAME_KEY);
        if (idNodeName == null) {
            throw new IOException("missing required property: "
                    + Configuration.ID_NAME_KEY);
        }
        if (idNodeName.equals(Configuration.ID_NAME_AUTO)) {
            logger.info("generating automatic ids");
        }

        // some or all of these may be null
        configureOptions();

        configureCollections();

        connectionStrings = props.getProperty(
                Configuration.CONNECTION_STRING_KEY,
                "admin:admin@localhost:9000").split("\\s+");
        logger.info("connecting to "
                + Utilities.join(connectionStrings, " "));

        // some config fields require extensive setup
        configurePlaceKeys();
    }

    private void configureOptions() {
        recordName = props
                .getProperty(Configuration.RECORD_NAME_KEY);
        recordNamespace = props
                .getProperty(Configuration.RECORD_NAMESPACE_KEY);
        if (recordName != null && recordNamespace == null)
            recordNamespace = "";

        ignoreUnknown = Utilities.stringToBoolean(props.getProperty(
                Configuration.IGNORE_UNKNOWN_KEY, "false"));

        // use prefix to set document-uri patterns
        uriPrefix = props.getProperty(
                Configuration.OUTPUT_URI_PREFIX_KEY, "");
        if (!uriPrefix.equals("") && !uriPrefix.endsWith("/")) {
            uriPrefix += "/";
        }
        uriSuffix = props.getProperty(
                Configuration.OUTPUT_URI_SUFFIX_KEY, "");

        // look for startId, to skip records
        startId = props
                .getProperty(Configuration.START_ID_KEY);
        logger.fine("START_ID=" + startId);

        // should we check for existing docs?
        skipExisting = Utilities.stringToBoolean(props.getProperty(
                Configuration.SKIP_EXISTING_KEY, "false"));
        logger.fine("SKIP_EXISTING=" + skipExisting);

        // should we throw an error for existing docs?
        errorExisting = Utilities.stringToBoolean(props.getProperty(
                Configuration.ERROR_EXISTING_KEY, "false"));
        logger.fine("ERROR_EXISTING=" + errorExisting);

        useAutomaticIds = Configuration.ID_NAME_AUTO
                .equals(props
                        .getProperty(Configuration.ID_NAME_KEY));
        logger.fine("useAutomaticIds=" + useAutomaticIds);

        String repairString = props.getProperty(
                Configuration.REPAIR_LEVEL_KEY, "" + "NONE");
        if (repairString.equals("FULL")) {
            logger.fine(Configuration.REPAIR_LEVEL_KEY
                    + "=FULL");
            repairLevel = XDMPDocInsertStream.XDMP_ERROR_CORRECTION_FULL;
        }

        fatalErrors = Utilities.stringToBoolean(props.getProperty(
                Configuration.FATAL_ERRORS_KEY, "true"));

        entityPolicy = props
                .getProperty(
                        Configuration.UNRESOLVED_ENTITY_POLICY_KEY,
                        Configuration.UNRESOLVED_ENTITY_POLICY_DEFAULT);

        inputEncoding = props.getProperty(
                Configuration.INPUT_ENCODING_KEY,
                DEFAULT_OUTPUT_ENCODING);
        malformedInputAction = props.getProperty(
                Configuration.INPUT_MALFORMED_ACTION_KEY,
                Configuration.INPUT_MALFORMED_ACTION_DEFAULT);
        logger.info("using output encoding " + DEFAULT_OUTPUT_ENCODING);

        threadCount = Integer.parseInt(props.getProperty(
                Configuration.THREADS_KEY, "1"));
        inputPath = props
                .getProperty(Configuration.INPUT_PATH_KEY);
        inputPattern = props
                .getProperty(Configuration.INPUT_PATTERN_KEY,
                        "^.+\\.xml$");
    }

    private void configureCollections() {
        // initialize collections
        List<String> collections = new ArrayList<String>();
        collections.add(RecordLoader.NAME + "."
                + System.currentTimeMillis());
        logger.info("adding extra collection: " + collections.get(0));
        String collectionsString = props
                .getProperty(Configuration.OUTPUT_COLLECTIONS_KEY);
        if (collectionsString != null && !collectionsString.equals("")) {
            collections.addAll(Arrays
                    .asList(collectionsString.split("[\\s,]+")));
        }
        // keep a base list of collections that can be extended later
        baseCollections = collections.toArray(new String[0]);
    }

    private void configurePlaceKeys() throws XDBCException {
        // if we use OUTPUT_FORESTS, we have to query for it!
        String placeKeysString = props.getProperty(OUTPUT_FORESTS_KEY);
        if (placeKeysString != null && !placeKeysString.equals("")) {
            logger.info("sending output to forest names: "
                    + placeKeysString);
            logger.fine("querying for Forest ids");
            XDBCResultSequence rs = null;
            String query = "define variable $forest-string as xs:string external\n"
                    + "for $fn in tokenize($forest-string, '[,:;\\s]+')\n"
                    + "return xs:string(xdmp:forest($fn))\n";
            // failures here are fatal
            Connection conn = new Connection(connectionStrings[0]);

            Map<String, String> externs = new Hashtable<String, String>(1);
            externs.put("forest-string", placeKeysString);
            rs = conn.executeQuery(query, externs);
            List<String> forestIds = new ArrayList<String>();
            while (rs.hasNext()) {
                rs.next();
                forestIds.add(rs.get_String());
            }
            props.setProperty(OUTPUT_FORESTS_KEY, Utilities.join(
                    forestIds, " "));
            logger.info("sending output to forests ids: "
                    + props.getProperty(OUTPUT_FORESTS_KEY));
        }

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

    public int getRepairLevel() {
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
        return repairLevel == XDMPDocOptions.XDMP_ERROR_CORRECTION_FULL;
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

    public String[] getConnectionStrings() {
        return connectionStrings;
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
    public XDMPPermission[] getPermissions() {
        XDMPPermission[] permissions = null;
        String readRolesString = props.getProperty(
                Configuration.OUTPUT_READ_ROLES_KEY, "");
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
        return permissions;
    }

    /**
     * @return
     */
    public String getOutputNamespace() {
        return props
                .getProperty(Configuration.DEFAULT_NAMESPACE_KEY);
    }

    /**
     * @return
     */
    public String[] getPlaceKeys() {
        // support placeKeys for Forest placement
        // comma-delimited string, also accept ;:\s
        String[] placeKeys = null;
        String placeKeysString = props
                .getProperty(Configuration.OUTPUT_FORESTS_KEY);
        if (placeKeysString != null) {
            placeKeysString = placeKeysString.trim();
            if (!placeKeysString.equals("")) {
                // numeric keys, so whitespace is enough
                placeKeys = placeKeysString.split("\\s+");
            }
        }
        return placeKeys;
    }

    /**
     * 
     * @throws XmlPullParserException @return
     */
    public XmlPullParserFactory getXppFactory() throws XmlPullParserException {
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

}
