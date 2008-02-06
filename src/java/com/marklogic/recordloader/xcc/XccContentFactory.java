/**
 * Copyright (c) 2008 Mark Logic Corporation. All rights reserved.
 */
package com.marklogic.recordloader.xcc;

import com.marklogic.recordloader.ContentInterface;
import com.marklogic.recordloader.LoaderException;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.exceptions.XccException;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class XccContentFactory extends XccAbstractContentFactory {

    private ContentCreateOptions options = null;

    protected void initOptions() throws XccException {
        // only initialize docOpts once
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
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.recordloader.ContentFactory#setFileBasename(java.lang.String)
     */
    public void setFileBasename(String _name) throws LoaderException {
        super.setFileBasename(_name);
        // update content options with the latest collections
        options.setCollections(collections.toArray(new String[0]));
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.recordloader.ContentFactory#newContent(java.lang.String)
     */
    public ContentInterface newContent(String _uri) {
        return new XccContent(cs.newSession(), _uri, options);
    }

    /* (non-Javadoc)
     * @see com.marklogic.recordloader.ContentFactory#close()
     */
    public void close() {
        // nothing to do
    }
}
