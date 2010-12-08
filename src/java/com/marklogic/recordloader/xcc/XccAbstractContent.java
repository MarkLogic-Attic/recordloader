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

import com.marklogic.recordloader.AbstractContent;
import com.marklogic.recordloader.ContentInterface;
import com.marklogic.xcc.Session;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public abstract class XccAbstractContent extends AbstractContent
        implements ContentInterface {

    Session session = null;

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

    /**
     * @return
     */
    public Session getSession() {
        return session;
    }

}
