/**
 * Copyright (c) 2008-2009 Mark Logic Corporation. All rights reserved.
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
import com.marklogic.xcc.DocumentFormat;
import com.marklogic.xcc.DocumentRepairLevel;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.XccConfigException;
import com.marklogic.xcc.exceptions.XccException;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class XccConfiguration extends Configuration {

    /**
     * 
     */
    public static final String CONNECTION_STRING_DEFAULT = "xcc://admin:admin@localhost:9000/";

    /**
     * 
     */
    public static final String DOCUMENT_FORMAT_DEFAULT = DocumentFormat.XML
            .toString();

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

    Object placeKeysMutex = new Object();

    Object metadataMutex = new Object();

    volatile BigInteger[] placeKeys;

    volatile ContentbaseMetaData metadata;

    int quality = 0;

    protected DocumentRepairLevel repairLevel = DocumentRepairLevel.NONE;

    DocumentFormat format = DocumentFormat.XML;

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
            if (null != metadata) {
                return metadata;
            }
            URI uri = getConnectionStrings()[0];
            ContentSource cs = ContentSourceFactory.newContentSource(uri);
            // be sure to use the default db
            Session session = cs.newSession();
            metadata = session.getContentbaseMetaData();
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
    public DocumentFormat getFormat() {
        return format;
    }

    /**
     * @return
     */
    public String getLanguage() {
        return properties.getProperty(LANGUAGE_KEY);
    }

    public DocumentRepairLevel getRepairLevel() {
        return repairLevel;
    }

    /**
     * @return
     */
    public boolean isFullRepair() {
        return DocumentRepairLevel.FULL == repairLevel;
    }

    @Override
    public void configure() {
        super.configure();

        // XCC-specific options
        logger.info("configuring XCC-specific options");

        String repairString = properties.getProperty(REPAIR_LEVEL_KEY);
        if (repairString.equals("FULL")) {
            repairLevel = DocumentRepairLevel.FULL;
        }

        String formatString = properties.getProperty(DOCUMENT_FORMAT_KEY)
                .toLowerCase();
        if (DocumentFormat.TEXT.toString().startsWith(formatString)) {
            format = DocumentFormat.TEXT;
        } else if (DocumentFormat.BINARY.toString().startsWith(
                formatString)) {
            format = DocumentFormat.BINARY;
        } else if (DocumentFormat.XML.toString().startsWith(formatString)) {
            format = DocumentFormat.XML;
        } else {
            logger.warning("Unexpected: " + DOCUMENT_FORMAT_KEY + "="
                    + formatString + " (using xml)");
            format = DocumentFormat.XML;
        }

        String forestNames = properties.getProperty(OUTPUT_FORESTS_KEY);
        if (null != forestNames) {
            forestNames = forestNames.trim();
            if (!forestNames.equals("")) {
                logger.info("sending output to forests: " + forestNames);
                logger.fine("querying for Forest ids");
                String[] placeNames = forestNames.split("\\s+");
                try {
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
                } catch (XccException e) {
                    throw new FatalException(e);
                }
            }
        }
    }

}
