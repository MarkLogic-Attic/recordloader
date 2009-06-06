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

import com.marklogic.recordloader.ContentFactory;
import com.marklogic.recordloader.ContentInterface;
import com.marklogic.recordloader.LoaderException;
import com.marklogic.xcc.exceptions.XccException;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class XccModuleContentFactory extends XccAbstractContentFactory
        implements ContentFactory {

    protected String moduleUri;

    protected String[] readRoles;

    protected String[] collectionsArray;

    protected String language;

    protected String namespace;

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.recordloader.xcc.XccAbstractContentFactory#close()
     */
    @Override
    public void close() {
        // nothing to do
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.marklogic.recordloader.xcc.XccAbstractContentFactory#initOptions()
     */
    @SuppressWarnings("unused")
    @Override
    protected void initOptions() throws XccException, LoaderException {
        moduleUri = configuration.getContentModuleUri();
        if (null == moduleUri) {
            throw new LoaderException("missing required property "
                    + XccConfiguration.CONTENT_MODULE_KEY);
        }
        readRoles = configuration.getReadRoles();
        collectionsArray = configuration.getBaseCollections();
        language = configuration.getLanguage();
        namespace = configuration.getOutputNamespace();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.marklogic.recordloader.xcc.XccAbstractContentFactory#newContent(java
     * .lang.String)
     */
    @SuppressWarnings("unused")
    @Override
    public ContentInterface newContent(String _uri)
            throws LoaderException {
        // TODO add isSkipExistingUntilFirstMiss
        return new XccModuleContent(cs.newSession(), _uri, moduleUri,
                readRoles, collectionsArray, language, namespace,
                configuration.isSkipExisting(), configuration
                        .isErrorExisting(), configuration.getDecoder());
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
