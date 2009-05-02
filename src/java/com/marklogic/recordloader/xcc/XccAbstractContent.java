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
        // boolean doc is actually cheaper than xdmp:exists doc
        String query = XQUERY_VERSION_0_9_ML
                + "define variable $URI as xs:string external\n"
                + "boolean(doc($URI))\n";
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
