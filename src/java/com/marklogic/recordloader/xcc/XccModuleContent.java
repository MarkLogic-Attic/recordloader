/**
 * Copyright (c) 2008 Mark Logic Corporation. All rights reserved.
 */
package com.marklogic.recordloader.xcc;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;

import com.marklogic.ps.Utilities;
import com.marklogic.recordloader.ContentInterface;
import com.marklogic.recordloader.LoaderException;
import com.marklogic.recordloader.Producer;
import com.marklogic.xcc.Request;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 * This implementation passes the XML source to a defined module. As such, it
 * cannot handle documents larger than available memory.
 * 
 * Also, this class can only handle XML, and possibly text: no binaries!
 * 
 */
public class XccModuleContent extends XccAbstractContent implements
        ContentInterface {

    private String xml = null;

    private Request request = null;

    private String[] roles;

    private String[] collections;

    /**
     * @param _session
     * @param _uri
     * @param _moduleUri
     * @param _collections
     */
    public XccModuleContent(Session _session, String _uri,
            String _moduleUri, String[] _roles, String[] _collections) {
        session = _session;
        uri = _uri;
        request = session.newModuleInvoke(_moduleUri);
        roles = _roles;
        collections = _collections;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.recordloader.ContentInterface#close()
     */
    public void close() {
        // not much to do
        xml = null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.recordloader.ContentInterface#insert()
     */
    public void insert() throws LoaderException {
        if (null == uri) {
            throw new LoaderException("URI cannot be null");
        }
        if (null == session) {
            throw new LoaderException("Session cannot be null");
        }
        if (null == request) {
            throw new LoaderException("Request cannot be null");
        }
        try {
            // always four variables: URI, XML-STRING, ROLES, COLLECTIONS
            request.setNewStringVariable("URI", uri);
            request.setNewStringVariable("XML-STRING", xml);
            request.setNewStringVariable("ROLES", joinCsv(roles));
            request.setNewStringVariable("COLLECTIONS",
                    joinCsv(collections));
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
     * @see com.marklogic.recordloader.ContentInterface#setProducer(com.marklogic.recordloader.Producer)
     */
    public void setProducer(Producer _producer) throws LoaderException {
        if (null == uri) {
            throw new LoaderException("URI cannot be null");
        }

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
    public void setXml(String _xml) throws LoaderException {
        xml = _xml;
    }

}
