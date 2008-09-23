/**
 * Copyright (c) 2008 Mark Logic Corporation. All rights reserved.
 */
package com.marklogic.recordloader.xcc;

import java.io.InputStream;

import com.marklogic.recordloader.ContentInterface;
import com.marklogic.recordloader.LoaderException;
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
public abstract class XccAbstractContent implements ContentInterface {

    /**
     * 
     */
    private static final String XQUERY_VERSION_0_9_ML = "xquery version \"0.9-ml\"\n";

    Session session = null;

    String uri = null;

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.recordloader.ContentInterface#close()
     */
    public void close() {
        if (null != session) {
            session.close();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.recordloader.ContentInterface#insert()
     */
    public abstract void insert() throws LoaderException;

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.recordloader.ContentInterface#setProducer(com.marklogic.recordloader.Producer)
     */
    public abstract void setInputStream(InputStream _producer)
            throws LoaderException;

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.recordloader.ContentInterface#setXml(java.lang.String)
     */
    public abstract void setBytes(byte[] _xml) throws LoaderException;

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.recordloader.ContentInterface#checkDocumentUri(java.lang.String)
     */
    public boolean checkDocumentUri(String _uri) throws LoaderException {
        String query = XQUERY_VERSION_0_9_ML
                + "define variable $URI as xs:string external\n"
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

    /**
     * @return
     */
    public Session getSession() {
        return session;
    }

}
