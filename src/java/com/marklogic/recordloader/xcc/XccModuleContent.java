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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;

import com.marklogic.ps.Utilities;
import com.marklogic.recordloader.ContentInterface;
import com.marklogic.recordloader.FatalException;
import com.marklogic.recordloader.LoaderException;
import com.marklogic.xcc.Request;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.types.ValueType;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 *         This implementation passes the XML source to a defined module. As
 *         such, it cannot handle documents larger than available memory.
 * 
 *         Also, this class can only handle XML, and possibly text: no binaries!
 * 
 */
public class XccModuleContent extends XccAbstractContent implements
        ContentInterface {

    protected String xml = null;

    protected Request request = null;

    protected String[] roles;

    protected String[] collections;

    protected String language;

    protected String namespace;

    private boolean skipExisting;

    private boolean errorExisting;

    /**
     * @param _session
     * @param _uri
     * @param _moduleUri
     * @param _collections
     * @param _language
     * @param _namespace
     * @param _skipExisting
     * @param _errorExisting
     */
    public XccModuleContent(Session _session, String _uri,
            String _moduleUri, String[] _roles, String[] _collections,
            String _language, String _namespace, boolean _skipExisting,
            boolean _errorExisting) {
        session = _session;
        uri = _uri;
        if (null == _moduleUri) {
            throw new FatalException("module URI cannot be null");
        }
        request = session.newModuleInvoke(_moduleUri);
        roles = _roles;
        collections = _collections;
        language = _language;
        namespace = _namespace;
        skipExisting = _skipExisting;
        errorExisting = _errorExisting;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.recordloader.ContentInterface#insert()
     */
    public void insert() throws LoaderException {
        if (null == uri) {
            throw new NullPointerException("URI cannot be null");
        }
        if (null == session) {
            throw new NullPointerException("Session cannot be null");
        }
        if (null == request) {
            throw new NullPointerException("Request cannot be null");
        }
        try {
            // always four variables: URI, XML-STRING, ROLES, COLLECTIONS
            // TODO add output forests?
            request.setNewStringVariable("URI", uri);
            request.setNewStringVariable("XML-STRING", xml);
            request.setNewStringVariable("NAMESPACE", namespace);
            request.setNewStringVariable("LANGUAGE",
                    (null == language) ? "" : language);
            request.setNewStringVariable("ROLES", joinCsv(roles));
            request.setNewStringVariable("COLLECTIONS",
                    joinCsv(collections));
            request.setNewVariable("SKIP-EXISTING", ValueType.XS_BOOLEAN,
                    skipExisting);
            request.setNewVariable("ERROR-EXISTING",
                    ValueType.XS_BOOLEAN, errorExisting);
            // ignore results
            session.submitRequest(request);
        } catch (RequestException e) {
            throw new LoaderException(e);
        }
    }

    /**
     * @param rolesCsv
     * @return
     */
    private String joinCsv(String[] values) {
        if (null == values) {
            return "";
        }
        return Utilities.join(values, ",");
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.marklogic.recordloader.ContentInterface#setProducer(com.marklogic
     * .recordloader.Producer)
     */
    public void setInputStream(InputStream _producer)
            throws LoaderException {
        if (null == uri) {
            throw new LoaderException("URI cannot be null");
        }

        // by now, the producer is in the configured output encoding
        Reader reader = new InputStreamReader(_producer);
        Writer writer = new StringWriter();
        char[] buf = new char[32 * 1024];
        int count = -1;
        try {
            // read the entire doc - this won't scale past the VM size
            while ((count = reader.read(buf)) > -1) {
                writer.write(buf, 0, count);
            }
            writer.flush();
            xml = writer.toString();
            writer.close();
            reader.close();
        } catch (IOException e) {
            throw new LoaderException(e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.recordloader.ContentInterface#setXml(java.lang.String)
     */
    @SuppressWarnings("unused")
    public void setBytes(byte[] _xml) throws LoaderException {
        // ModuleContent only works with strings
        // TODO support text? binary?
        xml = new String(_xml);
    }

    @SuppressWarnings("unused")
    @Override
    public boolean checkDocumentUri(String _uri) throws LoaderException {
        // override super(), so that we don't check the database.
        // checking the database does not work, as modules can rewrite uris.
        return false;
    }

}
