/*
 * Copyright (c)2004-2006 Mark Logic Corporation
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
package com.marklogic.ps;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.util.List;

/**
 * @author mike.blakeley@marklogic.com
 * 
 */
public class Utilities {

    private static final int BUFFER_SIZE = 32 * 1024;

    public static String join(List<String> _items, String _delim) {
        return join(_items.toArray(new String[0]), _delim);
    }

    /**
     * @param _items
     * @param _delim
     * @return
     */
    public static String join(String[] _items, String _delim) {
        String rval = "";
        for (int i = 0; i < _items.length; i++)
            if (i == 0)
                rval = _items[0];
            else
                rval += _delim + _items[i];
        return rval;
    }

    public static String escapeXml(String _in) {
        if (_in == null)
            return "";
        return _in.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(
                ">", "&gt;");
    }

    public static long copy(InputStream _in, OutputStream _out)
            throws IOException {
        if (_in == null)
            throw new IOException("null InputStream");
        if (_out == null)
            throw new IOException("null OutputStream");

        long totalBytes = 0;
        int len = 0;
        byte[] buf = new byte[BUFFER_SIZE];
        int available = _in.available();
        // System.err.println("DEBUG: " + _in + ": available " + available);
        while ((len = _in.read(buf, 0, BUFFER_SIZE)) > -1) {
            _out.write(buf, 0, len);
            totalBytes += len;
            // System.err.println("DEBUG: " + _out + ": wrote " + len);
        }
        // System.err.println("DEBUG: " + _in + ": last read " + len);

        // caller MUST close the stream for us
        _out.flush();

        // check to see if we copied enough data
        if (available > totalBytes)
            throw new IOException("expected at least " + available
                    + " Bytes, copied only " + totalBytes);

        return totalBytes;
    }

    /**
     * @param _in
     * @param _out
     * @throws IOException
     */
    public static void copy(File _in, File _out) throws IOException {
        InputStream in = new FileInputStream(_in);
        OutputStream out = new FileOutputStream(_out);
        copy(in, out);
    }

    public static long copy(Reader _in, OutputStream _out) throws IOException {
        if (_in == null)
            throw new IOException("null InputStream");
        if (_out == null)
            throw new IOException("null OutputStream");

        long totalBytes = 0;
        int len = 0;
        char[] buf = new char[BUFFER_SIZE];
        byte[] bite = null;
        while ((len = _in.read(buf)) > -1) {
            bite = new String(buf).getBytes();
            // len? different for char vs byte?
            // code is broken if I use bite.length, though
            _out.write(bite, 0, len);
            totalBytes += len;
        }

        // caller MUST close the stream for us
        _out.flush();

        // check to see if we copied enough data
        if (1 > totalBytes)
            throw new IOException("expected at least " + 1
                    + " Bytes, copied only " + totalBytes);

        return totalBytes;
    }

    /**
     * @param inFilePath
     * @param outFilePath
     * @throws IOException
     * @throws FileNotFoundException
     */
    public static void copy(String inFilePath, String outFilePath)
            throws FileNotFoundException, IOException {
        copy(new FileInputStream(inFilePath), new FileOutputStream(outFilePath));
    }

    public static final boolean stringToBoolean(String str) {
        return stringToBoolean(str, false);
    }

    // let the caller decide: should an unset string be true or false?
    public static final boolean stringToBoolean(String str, boolean defaultValue) {
        if (str == null)
            return defaultValue;

        String lcStr = str.toLowerCase();
        if (str.equals("") || str.equals("0") || lcStr.equals("f")
                || lcStr.equals("false") || lcStr.equals("n")
                || lcStr.equals("no"))
            return false;

        return true;
    }

}
