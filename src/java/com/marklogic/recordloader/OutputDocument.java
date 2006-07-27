/**
 * Copyright (c) 2006 Mark Logic Corporation. All rights reserved.
 */
package com.marklogic.recordloader;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import com.marklogic.ps.Connection;
import com.marklogic.ps.SimpleLogger;
import com.marklogic.xdbc.XDBCException;
import com.marklogic.xdmp.XDMPDocInsertStream;
import com.marklogic.xdmp.XDMPDocOptions;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class OutputDocument {
    private String uri = null;

    private StringBuffer prepend = null;

    private XDMPDocInsertStream stream = null;

    private String outputEncoding = Configuration.DEFAULT_OUTPUT_ENCODING;

    private SimpleLogger logger;

    private int bytesWritten = 0;

    /**
     * @param _conn
     * @param _uri
     * @param _opts
     * @throws IOException
     * @throws XDBCException
     * @throws UnsupportedEncodingException
     */
    public OutputDocument(SimpleLogger _logger,
            Connection _conn, String _uri, XDMPDocOptions _opts)
            throws UnsupportedEncodingException, XDBCException,
            IOException {
        logger = _logger;

        // open the docInsertStream for this attribute-id document
        open(_conn, _uri, _opts);
    }

    /**
     * @param _conn
     * @param _uri
     * @param _opts
     * @throws XDBCException
     * @throws IOException
     * @throws UnsupportedEncodingException
     */
    public void open(Connection _conn, String _uri, XDMPDocOptions _opts)
            throws XDBCException, UnsupportedEncodingException,
            IOException {
        uri = _uri;
        logger.finest("uri = \"" + uri + "\"");
        stream = _conn.openDocInsertStream(_uri, _opts);
        if (prepend != null && prepend.length() > 0) {
            write(prepend.toString());
        }
        prepend = null;
    }

    /**
     * @param _logger
     */
    public OutputDocument(SimpleLogger _logger) {
        // we don't know the URI yet, so we can't open the stream yet
        // so we'll buffer up the contents until we do...
        // note that we might simply throw this away, too
        logger = _logger;
        logger.finest("no uri");
        prepend = new StringBuffer();
    }

    /**
     * @return
     */
    public String getUri() {
        return uri;
    }

    /**
     * @param _string
     * @throws IOException
     * @throws UnsupportedEncodingException
     */
    public void write(String _string)
            throws UnsupportedEncodingException, IOException {
        if (stream == null) {
            if (prepend == null) {
                logger
                        .fine("skipping entity replacement: no current record");
                return;
            }
            prepend.append(_string);
            return;
        }
        byte[] bytes = _string.getBytes(outputEncoding);
        stream.write(bytes);
        bytesWritten += bytes.length;
    }

    public int getBytesWritten() {
        return bytesWritten;
    }

    /**
     * @throws XDBCException
     * 
     */
    public void abort() throws XDBCException {
        stream.abort();
    }

    /**
     * @throws IOException
     * 
     */
    public void flush() throws IOException {
        if (stream == null) {
            return;
        }
        
        stream.flush();
    }

    /**
     * @throws XDBCException
     * 
     */
    public void commit() throws XDBCException {
        if (stream == null) {
            throw new XDBCException("nothing to commit");
        }
        stream.commit();
    }

    /**
     * @throws IOException
     * 
     */
    public void close() throws IOException {
        if (stream != null) {
            stream.close();
        }
    }

    /**
     * @return
     */
    public boolean hasUri() {
        return uri != null;
    }
}
