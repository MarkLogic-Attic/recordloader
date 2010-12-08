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

import com.marklogic.recordloader.ContentFactory;
import com.marklogic.recordloader.ContentInterface;
import com.marklogic.recordloader.LoaderException;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class XccModuleContentFactory extends XccAbstractContentFactory
        implements ContentFactory {

    protected String moduleUri;

    protected String[] executeRoles;

    protected String[] insertRoles;

    protected String[] readRoles;

    protected String[] updateRoles;

    protected String[] collectionsArray;

    protected String language;

    protected String namespace;

    protected BigInteger[] placeKeys;
    
    protected int quality;

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.recordloader.xcc.XccAbstractContentFactory#close()
     */
    public void close() {
        // nothing to do
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.marklogic.recordloader.xcc.XccAbstractContentFactory#initOptions()
     */
    protected void initOptions() throws LoaderException {
        moduleUri = configuration.getContentModuleUri();
        if (null == moduleUri) {
            throw new LoaderException("missing required property "
                    + XccConfiguration.CONTENT_MODULE_KEY);
        }
        executeRoles = configuration.getExecuteRoles();
        insertRoles = configuration.getInsertRoles();
        readRoles = configuration.getReadRoles();
        updateRoles = configuration.getUpdateRoles();
        collectionsArray = configuration.getBaseCollections();
        language = configuration.getLanguage();
        namespace = configuration.getOutputNamespace();
        placeKeys = configuration.getPlaceKeys();
        quality = configuration.getQuality();
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
        // NB - this is closed in XccAbstractContent.close()
        return new XccModuleContent(cs.newSession(), _uri, moduleUri,
                executeRoles, insertRoles, readRoles, updateRoles,
                collectionsArray, language, namespace, configuration
                        .isSkipExisting(), configuration
                        .isSkipExistingUntilFirstMiss(), configuration
                        .isErrorExisting(), placeKeys, configuration
                        .getDecoder(), quality);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.marklogic.recordloader.ContentFactory#setFileBasename(java.lang.String
     * )
     */
    public void setFileBasename(String _name) throws LoaderException {
        super.setFileBasename(_name);
        // update content options with the latest collections
        collectionsArray = collections.toArray(new String[0]);
    }

}
