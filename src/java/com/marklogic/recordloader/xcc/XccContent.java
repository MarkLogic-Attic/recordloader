/**
 * Copyright (c) 2008 Mark Logic Corporation. All rights reserved.
 */
package com.marklogic.recordloader.xcc;

import com.marklogic.recordloader.ContentInterface;
import com.marklogic.recordloader.LoaderException;
import com.marklogic.recordloader.Producer;
import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentFactory;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.XccException;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class XccContent extends XccAbstractContent implements ContentInterface {

    Content content = null;

    ContentCreateOptions options = null;

    /**
     * @param _session
     * @param _uri
     * @param _options
     */
    public XccContent(Session _session, String _uri,
            ContentCreateOptions _options) {
        session = _session;
        uri = _uri;
        options = _options;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.recordloader.ContentInterface#insert()
     */
    public void insert() throws LoaderException {
        if (null == content.getUri()) {
            throw new LoaderException("URI cannot be null");
        }
        try {
            session.insertContent(content);
        } catch (XccException e) {
            throw new LoaderException(e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.recordloader.ContentInterface#setXml(java.lang.String)
     */
    public void setXml(String xml) throws LoaderException {
        if (null == uri) {
            throw new LoaderException("URI cannot be null");
        }
        content = ContentFactory.newContent(uri, xml, options);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.recordloader.ContentInterface#close()
     */
    public void close() {
        if (null != content) {
            content.close();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.recordloader.ContentInterface#setInput(com.marklogic.recordloader.Producer)
     */
    public void setProducer(Producer _producer) throws LoaderException {
        if (null == uri) {
            throw new LoaderException("URI cannot be null");
        }
        content = ContentFactory.newUnBufferedContent(uri, _producer,
                options);
    }

}