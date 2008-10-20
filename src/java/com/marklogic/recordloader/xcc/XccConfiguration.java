/**
 * Copyright (c) 2008 Mark Logic Corporation. All rights reserved.
 */
package com.marklogic.recordloader.xcc;

import java.math.BigInteger;
import java.net.URI;
import java.util.Map;

import com.marklogic.recordloader.Configuration;
import com.marklogic.recordloader.FatalException;
import com.marklogic.xcc.ContentPermission;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.ContentSourceFactory;
import com.marklogic.xcc.ContentbaseMetaData;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.XccConfigException;
import com.marklogic.xcc.exceptions.XccException;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class XccConfiguration extends Configuration {

    /**
     */
    public static final String OUTPUT_FORESTS_KEY = "OUTPUT_FORESTS";

    /**
     * 
     */
    public static final String OUTPUT_READ_ROLES_KEY = "READ_ROLES";

    /**
     * 
     */
    public static final String CONTENT_MODULE_KEY = "CONTENT_MODULE_URI";

    public static final String LANGUAGE_KEY = "LANGUAGE";

    public static final String CODE_TOOMANYSTANDS = "XDMP-TOOMANYSTANDS";

    BigInteger[] placeKeys;

    Object placeKeysMutex = new Object();

    ContentbaseMetaData metadata;

    Object metadataMutex = new Object();

    int quality = 0;

    /**
     * @return
     */
    public String getOutputNamespace() {
        return properties.getProperty(OUTPUT_NAMESPACE_KEY,
                OUTPUT_NAMESPACE_DEFAULT);
    }

    /**
     * @return
     */
    public ContentPermission[] getPermissions() {
        ContentPermission[] permissions = null;
        String[] readRoles = getReadRoles();
        if (readRoles != null && readRoles.length > 0) {
            permissions = new ContentPermission[readRoles.length];
            for (int i = 0; i < readRoles.length; i++) {
                if (readRoles[i] != null && !readRoles[i].equals(""))
                    permissions[i] = new ContentPermission(
                            ContentPermission.READ, readRoles[i]);
            }
        }
        return permissions;
    }

    /**
     * @return
     */
    public String[] getReadRoles() {
        String readRolesString = properties
                .getProperty(OUTPUT_READ_ROLES_KEY);
        if (null == readRolesString || readRolesString.length() < 1) {
            return null;
        }
        return readRolesString.trim().split("\\s+");
    }

    /**
     * @return
     * @throws XccException
     */
    public BigInteger[] getPlaceKeys() throws XccException {
        // lazy initialization
        if (null != placeKeys) {
            return placeKeys;
        }

        synchronized (placeKeysMutex) {
            String forestNames = properties
                    .getProperty(OUTPUT_FORESTS_KEY);
            // check again, to avoid any race for the mutex
            if (forestNames != null) {
                forestNames = forestNames.trim();
                if (!forestNames.equals("")) {
                    logger.info("sending output to forests: "
                            + forestNames);
                    logger.fine("querying for Forest ids");
                    String[] placeNames = forestNames.split("\\s+");
                    ContentbaseMetaData meta = getMetaData();
                    Map<?, ?> forestMap = meta.getForestMap();
                    placeKeys = new BigInteger[placeNames.length];
                    for (int i = 0; i < placeNames.length; i++) {
                        logger.finest("looking up " + placeNames[i]);
                        placeKeys[i] = (BigInteger) forestMap
                                .get(placeNames[i]);
                        if (null == placeKeys[i]) {
                            throw new FatalException("no forest named "
                                    + placeNames[i]);
                        }
                        logger.fine("mapping " + placeNames[i] + " to "
                                + placeKeys[i]);
                    }
                }
            }
        }
        return placeKeys;
    }

    /**
     * @return
     * @return
     * @throws XccConfigException
     */
    public ContentbaseMetaData getMetaData() throws XccConfigException {
        if (null != metadata) {
            return metadata;
        }
        synchronized (metadataMutex) {
            // check again, to prevent races
            if (null == metadata) {
                URI uri = getConnectionStrings()[0];
                ContentSource cs = ContentSourceFactory
                        .newContentSource(uri);
                // be sure to use the default db
                Session session = cs.newSession();
                metadata = session.getContentbaseMetaData();
            }
        }
        return metadata;
    }

    /**
     * @return
     */
    public int getQuality() {
        return quality;
    }

    /**
     * @return
     */
    public String getContentModuleUri() {
        return properties.getProperty(CONTENT_MODULE_KEY);
    }

    /**
     * @return
     */
    public String getLanguage() {
        return properties.getProperty(LANGUAGE_KEY);
    }

}
