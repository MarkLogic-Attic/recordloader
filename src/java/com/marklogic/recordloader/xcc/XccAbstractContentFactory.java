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

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.marklogic.ps.SimpleLogger;
import com.marklogic.recordloader.Configuration;
import com.marklogic.recordloader.ContentFactory;
import com.marklogic.recordloader.ContentInterface;
import com.marklogic.recordloader.FatalException;
import com.marklogic.recordloader.LoaderException;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.ContentSourceFactory;
import com.marklogic.xcc.ContentbaseMetaData;
import com.marklogic.xcc.exceptions.XccConfigException;
import com.marklogic.xcc.exceptions.XccException;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public abstract class XccAbstractContentFactory implements ContentFactory {

    protected XccConfiguration configuration;

    protected ContentSource cs;

    protected List<String> collections;

    protected SimpleLogger logger;

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.recordloader.ContentFactory#close()
     */
    public abstract void close();

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.recordloader.ContentFactory#getVersionString()
     */
    public String getVersionString() {
        try {
            ContentbaseMetaData meta = configuration.getMetaData();
            return "client = " + meta.getDriverVersionString()
                    + ", server = " + meta.getServerVersionString();
        } catch (XccException e) {
            throw new FatalException(e);
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.recordloader.ContentFactory#newContent(java.lang.String)
     */
    public abstract ContentInterface newContent(String _uri)
            throws LoaderException;

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.recordloader.ContentFactory#setProperties(java.util.Properties)
     */
    public void setConfiguration(Configuration _configuration)
            throws LoaderException {
        configuration = (XccConfiguration) _configuration;
        logger = configuration.getLogger();
        try {
            initOptions();
        } catch (XccException e) {
            throw new LoaderException(e);
        }
    }

    /**
     * @throws LoaderException
     * 
     */
    protected abstract void initOptions() throws XccException,
            LoaderException;

    /**
     * @param _uri
     * @throws LoaderException
     */
    public void setConnectionUri(URI _uri) throws LoaderException {
        // this is sort of redundant, but the Loader doesn't know which
        // round-robin index to use.
        try {
            cs = ContentSourceFactory.newContentSource(_uri);
        } catch (XccConfigException e) {
            throw new LoaderException(e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.recordloader.ContentFactory#setFileBasename(java.lang.String)
     */
    public void setFileBasename(String _name) throws LoaderException {
        // ensure that doc options exist
        try {
            initOptions();
        } catch (XccException e) {
            throw new LoaderException(e);
        }

        collections = new ArrayList<String>(Arrays.asList(configuration
                .getBaseCollections()));
        collections.add(_name);
    }

}
