/**
 * Copyright (c) 2008 Mark Logic Corporation. All rights reserved.
 */
package com.marklogic.recordloader.xcc;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.marklogic.recordloader.Configuration;
import com.marklogic.recordloader.ContentFactory;
import com.marklogic.recordloader.ContentInterface;
import com.marklogic.recordloader.FatalException;
import com.marklogic.recordloader.LoaderException;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.ContentSourceFactory;
import com.marklogic.xcc.ContentbaseMetaData;
import com.marklogic.xcc.exceptions.XccConfigException;
import com.marklogic.xcc.exceptions.XccException;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class XccContentFactory implements ContentFactory {

    private XccConfiguration configuration;

    private ContentCreateOptions options = null;

    private ContentSource cs;

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
     * @see com.marklogic.recordloader.ContentFactory#setProperties(java.util.Properties)
     */
    public void setConfiguration(Configuration _configuration)
            throws LoaderException {
        configuration = (XccConfiguration) _configuration;
        try {
            initDocumentOptions();
        } catch (XccException e) {
            throw new LoaderException(e);
        }
    }

    private void initDocumentOptions() throws XccException {
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
        // ensure that doc options exist
        try {
            initDocumentOptions();
        } catch (XccException e) {
            throw new LoaderException(e);
        }

        // update collections to include new basename
        List<String> newCollections = new ArrayList<String>(Arrays
                .asList(configuration.getBaseCollections()));
        newCollections.add(_name);
        options.setCollections(newCollections.toArray(new String[0]));
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.recordloader.ContentFactory#newContent(java.lang.String)
     */
    public ContentInterface newContent(String _uri) {
        return new XccContent(cs.newSession(), _uri, options);
    }

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

    /* (non-Javadoc)
     * @see com.marklogic.recordloader.ContentFactory#close()
     */
    public void close() {
        // nothing to do
    }
}
