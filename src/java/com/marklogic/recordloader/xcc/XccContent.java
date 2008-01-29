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
import com.marklogic.xcc.Request;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.XccException;
import com.marklogic.xcc.types.XSBoolean;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class XccContent implements ContentInterface {

    private Session session = null;

    private Content content = null;

    private String uri = null;

    private ContentCreateOptions options = null;

    /**
     * @param _session
     * @param _uri 
     * @param _options
     */
    public XccContent(Session _session, String _uri, ContentCreateOptions _options) {
        session = _session;
        uri = _uri;
        options = _options;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.recordloader.ContentInterface#checkDocumentUri(java.lang.String)
     */
    public boolean checkDocumentUri(String _uri) throws LoaderException {
        String query = "define variable $URI as xs:string external\n"
                + "xdmp:exists(doc($URI))\n";
        ResultSequence result = null;
        boolean exists = false;
        try {
            Request request = session.newAdhocQuery(query);
            request.setNewStringVariable("URI", _uri);

            result = session.submitRequest(request);

            if (!result.hasNext()) {
                throw new RequestException("unexpected null result",
                        request);
            }

            ResultItem item = result.next();

            exists = ((XSBoolean) item.getItem()).asPrimitiveBoolean();
        } catch (XccException e) {
            throw new LoaderException(e);
        } finally {
            if (result != null && !result.isClosed())
                result.close();
        }
        return exists;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.recordloader.ContentInterface#setUri(java.lang.String)
     */
    public void setUri(String _uri) {
        uri = _uri;
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
        content.close();
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