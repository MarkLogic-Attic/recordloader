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
package com.marklogic.recordloader.http;

import java.net.Authenticator;
import java.net.MalformedURLException;
import java.net.PasswordAuthentication;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.marklogic.ps.SimpleLogger;
import com.marklogic.recordloader.Configuration;
import com.marklogic.recordloader.ContentFactory;
import com.marklogic.recordloader.ContentInterface;
import com.marklogic.recordloader.LoaderException;

/**
 * @author Michael Blakeley, Mark Logic Corporation
 * 
 */
public class HttpModuleContentFactory implements ContentFactory {

    protected Configuration configuration;

    protected List<String> collections;

    protected SimpleLogger logger;

    protected String[] executeRoles;

    protected String[] insertRoles;

    protected String[] readRoles;

    protected String[] updateRoles;

    protected String[] collectionsArray;

    protected String language;

    protected String namespace;

    protected String[] placeKeys;

    protected URL connectionUrl;

    protected boolean authorityIsInitialized;

    /**
     * @throws LoaderException
     */
    protected void initOptions() throws LoaderException {
        executeRoles = configuration.getExecuteRoles();
        insertRoles = configuration.getInsertRoles();
        readRoles = configuration.getReadRoles();
        updateRoles = configuration.getUpdateRoles();
        collectionsArray = configuration.getBaseCollections();
        language = configuration.getLanguage();
        namespace = configuration.getOutputNamespace();
        // NB - it is up to the module to get forests from forest names
        placeKeys = configuration.getOutputForests();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.marklogic.recordloader.xcc.XccAbstractContentFactory#newContent(java
     * .lang.String)
     */
    @SuppressWarnings("unused")
    public ContentInterface newContent(String _uri)
            throws LoaderException {
        // TODO add isSkipExistingUntilFirstMiss
        return new HttpModuleContent(connectionUrl, _uri, executeRoles,
                insertRoles, readRoles, updateRoles, collectionsArray,
                language, namespace, configuration.isSkipExisting(),
                configuration.isErrorExisting(), placeKeys, configuration
                        .getDecoder());
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.marklogic.recordloader.ContentFactory#setFileBasename(java.lang.String
     * )
     */
    public void setFileBasename(String _name) throws LoaderException {
        // ensure that doc options exist
        initOptions();

        collections = new ArrayList<String>(Arrays.asList(configuration
                .getBaseCollections()));
        collections.add(_name);

        // update content options with the latest collections
        collectionsArray = collections.toArray(new String[0]);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.recordloader.ContentFactory#getVersionString()
     */
    public String getVersionString() {
        return "n/a";
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.marklogic.recordloader.ContentFactory#setProperties(java.util.Properties
     * )
     */
    public void setConfiguration(Configuration _configuration)
            throws LoaderException {
        configuration = _configuration;
        logger = configuration.getLogger();
        initOptions();
    }

    /**
     * @param _uri
     * @throws LoaderException
     */
    public void setConnectionUri(URI _uri) throws LoaderException {
        // this is sort of redundant, but the Loader doesn't know which
        // round-robin index to use.
        if (null == connectionUrl || !authorityIsInitialized) {
            try {
                final String[] auth = _uri.toURL().getAuthority().split(
                        "@")[0].split(":");
                Authenticator.setDefault(new Authenticator() {
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(auth[0],
                                auth[1].toCharArray());
                    }
                });
                authorityIsInitialized = true;
            } catch (MalformedURLException e) {
                throw new LoaderException(e);
            }
        }
        try {
            connectionUrl = _uri.toURL();
        } catch (MalformedURLException e) {
            throw new LoaderException(_uri.toString(), e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.recordloader.ContentInterface#close()
     */
    public void close() {
        // nothing to do
    }

}
