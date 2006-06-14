/**
 * Copyright (c) 2006 Mark Logic Corporation. All rights reserved.
 */
package com.marklogic.ps;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import com.marklogic.xdbc.XDBCException;
import com.marklogic.xdmp.XDMPDocInsertStream;
import com.marklogic.xdmp.XDMPDocOptions;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class RecordLoaderOutputDocument {
    private String uri = null;

    private StringBuffer prepend = null;

    private XDMPDocInsertStream stream = null;

    private String outputEncoding = RecordLoaderConfiguration.DEFAULT_OUTPUT_ENCODING;

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
    public RecordLoaderOutputDocument(SimpleLogger _logger,
            Connection _conn, String _uri, XDMPDocOptions _opts)
            throws UnsupportedEncodingException, XDBCException,
            IOException {
        uri = _uri;
        logger = _logger;

        // open the docInsertStream for this attribute-id document
        open(_conn, uri, _opts);
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
        stream = _conn.openDocInsertStream(_uri, _opts);
        if (prepend != null && prepend.length() > 0) {
            write(prepend.toString());
        }
        prepend = null;
    }

    /**
     * @param _logger
     */
    public RecordLoaderOutputDocument(SimpleLogger _logger) {
        // we don't know the URI yet, so we can't open the stream yet
        // so we'll buffer up the contents until we do...
        // note that we might simply throw this away, too
        logger = _logger;
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
        bytesWritten = bytes.length;
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
        stream.flush();
    }

    /**
     * @throws XDBCException
     * 
     */
    public void commit() throws XDBCException {
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
