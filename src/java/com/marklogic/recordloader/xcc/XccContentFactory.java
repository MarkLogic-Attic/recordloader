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

import com.marklogic.recordloader.ContentFactory;
import com.marklogic.recordloader.ContentInterface;
import com.marklogic.recordloader.LoaderException;
import com.marklogic.xcc.ContentCreateOptions;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class XccContentFactory extends XccAbstractContentFactory
        implements ContentFactory {

    protected ContentCreateOptions options = null;

    protected static Object optionsMutex = new Object();

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.marklogic.recordloader.xcc.XccAbstractContentFactory#initOptions()
     */
    protected void initOptions() {
        // only initialize docOpts once
        if (null != options) {
            return;
        }
        synchronized (optionsMutex) {
            if (null != options) {
                return;
            }
            boolean resolveEntities = false;
            options = new ContentCreateOptions();
            options.setResolveEntities(resolveEntities);
            options.setPermissions(configuration.getPermissions());
            options.setCollections(configuration.getBaseCollections());
            options.setQuality(configuration.getQuality());
            options.setNamespace(configuration.getOutputNamespace());
            options.setRepairLevel(configuration.getRepairLevel());
            options.setPlaceKeys(configuration.getPlaceKeys());
            options.setFormat(configuration.getFormat());
            options.setLanguage(configuration.getLanguage());

            // are we handling the encoding, or is the server doing it?
            if (!configuration.isLoaderTranscoding()) {
                logger.fine("server encoding "
                        + configuration.getInputEncoding());
                options.setEncoding(configuration.getInputEncoding());
            } else {
                logger.fine("transcoding loader - will not set encoding");
            }
        }
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
        options.setCollections(collections.toArray(new String[0]));
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.marklogic.recordloader.ContentFactory#newContent(java.lang.String)
     */
    public ContentInterface newContent(String _uri) {
        // NB - this is closed in XccAbstractContent.close()
        return new XccContent(cs.newSession(), _uri, options);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.recordloader.ContentFactory#close()
     */
    public void close() {
        // nothing to do
    }
}
