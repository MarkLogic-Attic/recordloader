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
package com.marklogic.recordloader.xcc;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;

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

    protected String[] executeRoles;

    protected String[] insertRoles;

    protected String[] readRoles;

    protected String[] updateRoles;

    protected String[] collections;

    protected String[] placeKeys;

    protected String language;

    protected String namespace;

    protected boolean skipExisting;

    protected boolean skipExistingUntilFirstMiss;

    protected boolean errorExisting;

    protected CharsetDecoder decoder;

    protected long quality;

    /**
     * @param _session
     * @param _uri
     * @param _moduleUri
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
     * @param _quality
     */
    public XccModuleContent(Session _session, String _uri,
            String _moduleUri, String[] _executeRoles,
            String[] _insertRoles, String[] _readRoles,
            String[] _updateRoles, String[] _collections,
            String _language, String _namespace, boolean _skipExisting,
            boolean _skipExistingUntilFirstMiss, boolean _errorExisting,
            BigInteger[] _placeKeys, CharsetDecoder _decoder,
            long _quality) {
        session = _session;
        uri = _uri;
        if (null == _moduleUri) {
            throw new FatalException("module URI cannot be null");
        }
        request = session.newModuleInvoke(_moduleUri);
        executeRoles = _executeRoles;
        insertRoles = _insertRoles;
        readRoles = _readRoles;
        updateRoles = _updateRoles;
        collections = _collections;
        language = _language;
        quality = _quality;
        namespace = _namespace;
        skipExisting = _skipExisting;
        skipExistingUntilFirstMiss = _skipExistingUntilFirstMiss;
        errorExisting = _errorExisting;
        decoder = _decoder;
        if (null == _placeKeys) {
            placeKeys = new String[0];
        } else {
            placeKeys = new String[_placeKeys.length];
            for (int i = 0; i < _placeKeys.length; i++) {
                placeKeys[i] = "" + _placeKeys[i];
            }
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
        if (null == session) {
            throw new NullPointerException("Session cannot be null");
        }
        if (null == request) {
            throw new NullPointerException("Request cannot be null");
        }
        try {
            request.setNewStringVariable("URI", uri);
            request.setNewStringVariable("XML-STRING", xml);
            request.setNewStringVariable("NAMESPACE", namespace);
            request.setNewStringVariable("LANGUAGE",
                    (null == language) ? "" : language);
            request.setNewStringVariable("ROLES-EXECUTE", Utilities
                    .joinSsv(executeRoles));
            request.setNewStringVariable("ROLES-INSERT", Utilities
                    .joinSsv(insertRoles));
            request.setNewStringVariable("ROLES-READ", Utilities
                    .joinSsv(readRoles));
            request.setNewStringVariable("ROLES-UPDATE", Utilities
                    .joinSsv(updateRoles));
            request.setNewStringVariable("COLLECTIONS", Utilities
                    .joinCsv(collections));
            request.setNewVariable("SKIP-EXISTING", ValueType.XS_BOOLEAN,
                    skipExisting);
            request.setNewVariable("SKIP-EXISTING-UNTIL-FIRST-MISS",
                    ValueType.XS_BOOLEAN, skipExisting);
            request.setNewVariable("ERROR-EXISTING",
                    ValueType.XS_BOOLEAN, errorExisting);
            // apparently it is ok if OUTPUT_FORESTS are empty (tested 4.1-1)
            request.setNewStringVariable("FORESTS", Utilities
                    .joinCsv(placeKeys));
            request.setNewIntegerVariable("QUALITY", quality);
            // ignore results
            // TODO use results to handle skipExistingUntilFirstMiss - how to
            // feed back to config?
            session.submitRequest(request);
//                synchronized (monitor) {
//                    logger.info("resetting "
//                            + Configuration.SKIP_EXISTING_KEY + " at "
//                            + uri);
//                    config.setSkipExisting(false);
//                    config.configureThrottling();
//                    monitor.resetTimer("skipped");
//                }
        } catch (RequestException e) {
            throw new LoaderException(e);
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
        // override super(), so that we don't check the database.
        // checking the database does not work, as modules can rewrite uris.
        return false;
    }

}
