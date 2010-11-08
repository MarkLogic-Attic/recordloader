/**
 * Copyright (c) 2008-2010 Mark Logic Corporation. All rights reserved.
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
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import com.marklogic.recordloader.Configuration;
import com.marklogic.recordloader.FatalException;
import com.marklogic.recordloader.FileLoader;
import com.marklogic.xcc.ContentCapability;
import com.marklogic.xcc.ContentPermission;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.ContentSourceFactory;
import com.marklogic.xcc.ContentbaseMetaData;
import com.marklogic.xcc.DocumentFormat;
import com.marklogic.xcc.DocumentRepairLevel;
import com.marklogic.xcc.SecurityOptions;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.XccConfigException;
import com.marklogic.xcc.exceptions.XccException;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class XccConfiguration extends Configuration {

    public static final String CONNECTION_STRING_DEFAULT = "xcc://admin:admin@localhost:9000/";

    public static final String CONTENT_MODULE_KEY = "CONTENT_MODULE_URI";

    public static final String DOCUMENT_FORMAT_DEFAULT = DocumentFormat.XML
            .toString();

    public static final String QUALITY_KEY = "OUTPUT_QUALITY";

    public static final String QUALITY_DEFAULT = "0";

    Object metadataMutex = new Object();

    volatile BigInteger[] placeKeys;

    volatile ContentbaseMetaData metadata;

    protected SecurityOptions securityOptions = null;

    Object securityOptionsMutex = new Object();

    protected DocumentRepairLevel repairLevel = DocumentRepairLevel.NONE;

    protected DocumentFormat format = DocumentFormat.XML;

    /**
     * @return
     * @throws XccException
     */
    public BigInteger[] getPlaceKeys() {
        return placeKeys;
    }

    /**
     * @return
     * @return
     * @throws XccConfigException
     * @throws NoSuchAlgorithmException
     * @throws KeyManagementException
     */
    public void initMetaData() throws XccConfigException,
            KeyManagementException, NoSuchAlgorithmException {
        if (null != metadata) {
            return;
        }
        synchronized (metadataMutex) {
            // check again, to prevent races
            if (null != metadata) {
                return;
            }
            URI uri = getConnectionStrings()[0];
            // support SSL or plain-text
            ContentSource cs = isSecure(uri) ? ContentSourceFactory
                    .newContentSource(uri, getSecurityOptions())
                    : ContentSourceFactory.newContentSource(uri);
            // be sure to use the default db
            Session session = cs.newSession();
            metadata = session.getContentbaseMetaData();
            // NB - this session is closed in the close() method
        }
    }

    @Override
    public void configure() {
        super.configure();

        // XCC-specific options
        logger.info("configuring XCC-specific options");

        String repairString = properties.getProperty(REPAIR_LEVEL_KEY);
        if (repairString.equalsIgnoreCase("FULL")) {
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

        String[] placeNames = getOutputForests();
        if (null != placeNames) {
            try {
                initMetaData();
                Map<?, ?> forestMap = metadata.getForestMap();
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
            } catch (KeyManagementException e) {
                throw new FatalException(e);
            } catch (NoSuchAlgorithmException e) {
                throw new FatalException(e);
            }
        }

    }

    protected static SecurityOptions newTrustAnyoneOptions()
            throws KeyManagementException, NoSuchAlgorithmException {
        TrustManager[] trust = new TrustManager[] { new X509TrustManager() {
            public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[0];
            }

            /**
             * @throws CertificateException
             */
            public void checkClientTrusted(
                    java.security.cert.X509Certificate[] certs,
                    String authType) throws CertificateException {
                // no exception means it's okay
            }

            /**
             * @throws CertificateException
             */
            public void checkServerTrusted(
                    java.security.cert.X509Certificate[] certs,
                    String authType) throws CertificateException {
                // no exception means it's okay
            }
        } };

        SSLContext sslContext = SSLContext.getInstance("SSLv3");
        sslContext.init(null, trust, null);
        return new SecurityOptions(sslContext);
    }

    /**
     * @return
     * @throws NoSuchAlgorithmException
     * @throws KeyManagementException
     */
    public SecurityOptions getSecurityOptions()
            throws KeyManagementException, NoSuchAlgorithmException {
        if (null != securityOptions) {
            return securityOptions;
        }
        synchronized (securityOptionsMutex) {
            if (null != securityOptions) {
                return securityOptions;
            }
            securityOptions = newTrustAnyoneOptions();
            return securityOptions;
        }
    }

    /**
     * @param _uri
     * @return
     */
    public boolean isSecure(URI _uri) {
        // TODO would be nice if XCC exposed this string
        return _uri.getScheme().equals("xccs");
    }

    public DocumentRepairLevel getRepairLevel() {
        return repairLevel;
    }

    /**
     * @return
     * 
     *         TODO - used only by XccContent. For XccModuleContent, we must
     *         break module API again.
     */
    public int getQuality() {
        return Integer.parseInt(properties.getProperty(QUALITY_KEY));
    }

    /**
     * @return
     */
    public boolean isFullRepair() {
        return DocumentRepairLevel.FULL == repairLevel;
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
    public ContentPermission[] getPermissions() {
        List<ContentPermission> permissions = new LinkedList<ContentPermission>();
        buildPermissions(permissions, ContentPermission.EXECUTE,
                getExecuteRoles());
        buildPermissions(permissions, ContentPermission.INSERT,
                getInsertRoles());
        buildPermissions(permissions, ContentPermission.READ,
                getReadRoles());
        buildPermissions(permissions, ContentPermission.UPDATE,
                getUpdateRoles());

        logger.fine("returning " + permissions.size());
        if (0 == permissions.size()) {
            return null;
        }
        return permissions.toArray(new ContentPermission[0]);
    }

    /**
     * @param permissions
     * @param capability
     * @param roles
     */
    private void buildPermissions(List<ContentPermission> permissions,
            ContentCapability capability, String[] roles) {
        logger.fine("processing "
                + (null == roles ? roles : roles.length));
        if (roles == null || roles.length < 1) {
            return;
        }
        String name;
        for (int i = 0; i < roles.length; i++) {
            name = roles[i];
            if (null == name) {
                continue;
            }
            name = name.trim();
            if ("".equals(name)) {
                continue;
            }
            logger.finer("adding " + capability + " for " + name);
            permissions.add(new ContentPermission(capability, name));
        }
    }

    /**
     * @return
     */
    public String getContentModuleUri() {
        return properties.getProperty(CONTENT_MODULE_KEY);
    }

    /**
     * @return
     * @throws NoSuchAlgorithmException
     * @throws XccConfigException
     * @throws KeyManagementException
     */
    public String getDriverVersionString() throws KeyManagementException,
            XccConfigException, NoSuchAlgorithmException {
        initMetaData();
        return metadata.getDriverVersionString();
    }

    /**
     * @return
     * @throws XccException
     * @throws NoSuchAlgorithmException
     * @throws KeyManagementException
     */
    public String getServerVersionString() throws XccException,
            KeyManagementException, NoSuchAlgorithmException {
        initMetaData();
        return metadata.getServerVersionString();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.recordloader.AbstractConfiguration#close()
     */
    @Override
    public void close() {
        if (null != metadata) {
            metadata.getSession().close();
        }
    }

}
