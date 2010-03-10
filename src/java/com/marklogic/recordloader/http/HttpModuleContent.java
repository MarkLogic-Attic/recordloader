/**
 * Copyright (c) 2008-2010 Mark Logic Corporation. All rights reserved.
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
package com.marklogic.recordloader.http;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.util.Arrays;

import com.marklogic.recordloader.AbstractContent;
import com.marklogic.recordloader.ContentInterface;
import com.marklogic.recordloader.LoaderException;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 *         This implementation passes the XML source to a defined module. As
 *         such, it cannot handle documents larger than available memory.
 * 
 *         Also, this class can only handle XML, and possibly text: no binaries!
 * 
 */
/**
 * @author Michael Blakeley, Mark Logic Corporation
 * 
 */
public class HttpModuleContent extends AbstractContent implements
        ContentInterface {

    protected String xml = null;

    protected String[] executeRoles;

    protected String[] insertRoles;

    protected String[] readRoles;

    protected String[] updateRoles;

    protected String[] collections;

    protected String[] placeKeys;

    protected String language;

    protected String namespace;

    protected boolean skipExisting;

    protected boolean errorExisting;

    protected CharsetDecoder decoder;

    protected String uri;

    private URL connectionUrl;

    /**
     * @param _connectionUrl
     * @param _uri
     * @param _executeRoles
     * @param _insertRoles
     * @param _readRoles
     * @param _updateRoles
     * @param _collections
     * @param _language
     * @param _namespace
     * @param _skipExisting
     * @param _errorExisting
     * @param _placeKeys
     * @param _decoder
     */
    public HttpModuleContent(URL _connectionUrl, String _uri,
            String[] _executeRoles, String[] _insertRoles,
            String[] _readRoles, String[] _updateRoles,
            String[] _collections, String _language, String _namespace,
            boolean _skipExisting, boolean _errorExisting,
            String[] _placeKeys, CharsetDecoder _decoder) {
        connectionUrl = _connectionUrl;
        uri = _uri;
        executeRoles = _executeRoles;
        insertRoles = _insertRoles;
        readRoles = _readRoles;
        updateRoles = _updateRoles;
        collections = _collections;
        language = _language;
        namespace = _namespace;
        skipExisting = _skipExisting;
        errorExisting = _errorExisting;
        decoder = _decoder;
        if (null == _placeKeys) {
            placeKeys = new String[0];
        } else {
            placeKeys = Arrays.copyOf(_placeKeys, _placeKeys.length);
        }
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

        StringBuilder body = new StringBuilder();
        try {
            append(body, "URI", uri);
            append(body, "XML-STRING", xml);
            append(body, "NAMESPACE", namespace);
            append(body, "LANGUAGE", (null == language) ? "" : language);
            append(body, "ROLES-EXECUTE", executeRoles);
            append(body, "ROLES-INSERT", insertRoles);
            append(body, "ROLES-READ", readRoles);
            append(body, "ROLES-UPDATE", updateRoles);
            append(body, "COLLECTIONS", collections);
            // TODO skip existing until first miss
            append(body, "SKIP-EXISTING", Boolean.toString(skipExisting));
            append(body, "ERROR-EXISTING", Boolean
                    .toString(errorExisting));
            append(body, "FORESTS", placeKeys);

            HttpURLConnection conn = (HttpURLConnection) connectionUrl
                    .openConnection();
            conn.setUseCaches(false);
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type",
                    "application/x-www-form-urlencoded");
            // use keep-alive to reduce open sockets
            conn.setRequestProperty("Connection", "keep-alive");
            conn.setDoOutput(true);
            OutputStreamWriter osw = new OutputStreamWriter(conn
                    .getOutputStream());
            osw.write(body.toString());
            osw.flush();

            BufferedReader in = new BufferedReader(new InputStreamReader(
                    conn.getInputStream()));
            String line;
            while ((line = in.readLine()) != null) {
                System.out.println(line);
            }
            in.close();

            return;
        } catch (IOException e) {
            throw new LoaderException(e);
        }
    }

    /**
     * @param _query
     * @param _key
     * @param _value
     * @throws UnsupportedEncodingException
     */
    private void append(StringBuilder _query, String _key, String _value)
            throws UnsupportedEncodingException {
        append(_query, _key, new String[] { _value });
    }

    /**
     * @param _query
     * @param _key
     * @param _value
     * @throws UnsupportedEncodingException
     */
    private void append(StringBuilder _query, String _key, String[] _value)
            throws UnsupportedEncodingException {
        if (null == _value || 1 > _value.length) {
            return;
        }
        for (int i = 0; i < _value.length; i++) {
            if (_query.length() > 0) {
                _query.append("&");
            }
            _query.append(_key).append("=").append(
                    URLEncoder.encode(_value[i], "UTF-8"));
        }
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

        // by now, the producer is in the configured output encoding,
        // so no decoder is needed
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
    public void setBytes(byte[] _xml) throws LoaderException {
        // ModuleContent only works with strings
        // TODO support text? binary?
        try {
            // use the correct decoder
            xml = decoder.decode(ByteBuffer.wrap(_xml)).toString();
        } catch (CharacterCodingException e) {
            throw new LoaderException(uri, e);
        }
    }

    @SuppressWarnings("unused")
    public boolean checkDocumentUri(String _uri) throws LoaderException {
        // checking the database does not work, as modules can rewrite uris.
        // instead, we provide the value of SKIP_EXISTING to the module.
        // given the information, it can do as it likes.
        return false;
    }

}
