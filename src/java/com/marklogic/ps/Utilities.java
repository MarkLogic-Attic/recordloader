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

import java.util.List;

/**
 * @author mike.blakeley@marklogic.com
 * 
 */
public class Utilities {

    public static String join(List<String> _items, String _delim) {
        return join(_items.toArray(new String[0]), _delim);
    }

    /**
     * @param _items
     * @param _delim
     * @return
     */
    public static String join(String[] _items, String _delim) {
        StringBuffer rval = new StringBuffer();
        for (int i = 0; i < _items.length; i++) {
            if (i == 0) {
                rval.append(_items[0]);
            } else {
                rval.append(_delim).append(_items[i]);
            }
        }
        return rval.toString();
    }

    /**
     * @param _items
     * @param _delim
     * @return
     */
    public static String join(Object[] _items, String _delim) {
        StringBuffer rval = new StringBuffer();
        for (int i = 0; i < _items.length; i++) {
            if (i == 0) {
                rval.append(_items[0]);
            } else {
                rval.append(_delim).append(_items[i]);
            }
        }
        return rval.toString();
    }

    public static String escapeXml(String _in) {
        if (_in == null)
            return "";
        return _in.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(
                ">", "&gt;");
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

    /**
     * @param t
     * @return
     */
    public static Throwable getCause(Throwable t) {
        // trace back to the original cause, if there was one
        Throwable cause = t;
        Throwable temp = null;
        while ((temp = cause.getCause()) != null) {
            cause = temp;
        }
        return cause;
    }

}
