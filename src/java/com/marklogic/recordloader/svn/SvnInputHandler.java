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
package com.marklogic.recordloader.svn;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import org.tmatesoft.svn.core.SVNDirEntry;
import org.tmatesoft.svn.core.SVNException;
import org.tmatesoft.svn.core.SVNNodeKind;
import org.tmatesoft.svn.core.SVNURL;
import org.tmatesoft.svn.core.auth.ISVNAuthenticationManager;
import org.tmatesoft.svn.core.internal.io.dav.DAVRepositoryFactory;
import org.tmatesoft.svn.core.io.SVNRepository;
import org.tmatesoft.svn.core.io.SVNRepositoryFactory;
import org.tmatesoft.svn.core.wc.SVNWCUtil;

import com.marklogic.recordloader.AbstractInputHandler;
import com.marklogic.recordloader.Configuration;
import com.marklogic.recordloader.FatalException;
import com.marklogic.recordloader.LoaderException;
import com.marklogic.recordloader.LoaderInterface;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 *
 */
public class SvnInputHandler extends AbstractInputHandler {

    private long revision;

    /*
     * (non-Javadoc)
     *
     * @see com.marklogic.recordloader.AbstractInputHandler#run()
     */
    @Override
    public void run() throws LoaderException {
        getFactory();

        // alternatively, treat first arg as repository url?
        if (0 < inputs.length) {
            logger.warning("ignoring inputs: "
                    + Arrays.deepToString(inputs));
        }

        String url = config.getInputPath();
        if (null == url) {
            throw new FatalException("missing required property: "
                    + Configuration.INPUT_PATH_KEY
                    + " must be an svn repository url");
        }
        logger.info("Reading from svn repository " + url);

        // TODO add svnkit.jar to ant files
        DAVRepositoryFactory.setup();
        SVNRepository repository = null;

        try {
            repository = SVNRepositoryFactory.create(SVNURL
                    .parseURIEncoded(url));
            ISVNAuthenticationManager am = SVNWCUtil
                    .createDefaultAuthenticationManager();
            repository.setAuthenticationManager(am);
            // TODO support configurable revisions
            revision = repository.getLatestRevision();

            logger.info("root = " + repository.getRepositoryRoot(true));
            logger.info("UUID = " + repository.getRepositoryUUID(true));
            logger.info("revision = " + revision);

            SVNNodeKind nodeKind = repository.checkPath("", revision);
            if (nodeKind == SVNNodeKind.NONE) {
                throw new FatalException("no entry at '" + url + "'.");
            } else if (nodeKind == SVNNodeKind.FILE) {
                throw new FatalException(url + " is a file");
            }
            listEntries(repository, "");
        } catch (SVNException e) {
            e.printStackTrace();
        } finally {
            if (null != repository) {
                try {
                    repository.closeSession();
                } catch (SVNException e) {
                    logger.logException("couldn't close repository session", e);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void listEntries(SVNRepository repository, String path)
            throws LoaderException, SVNException {
        Collection<SVNDirEntry> entries = repository.getDir(path,
                revision, null, (Collection<?>) null);
        Iterator<SVNDirEntry> iterator = entries.iterator();
        while (iterator.hasNext()) {
            SVNDirEntry entry = iterator.next();
            String name = entry.getRelativePath();
            String fullPath = path + ("".equals(path) ? "" : "/") + name;
            String rootedPath = "/" + fullPath;
            SVNNodeKind kind = entry.getKind();
            logger.finer(rootedPath + " is a " + kind);
            if (SVNNodeKind.DIR == kind) {
                listEntries(repository, fullPath);
                continue;
            }
            try {
                // seems to be important to *not* use rootedPath
                submit(repository, fullPath, rootedPath);
            } catch (SVNException e) {
                if (config.isFatalErrors()) {
                    throw e;
                }
                logger.logException(e);
                continue;
            }
        }
    }

    /**
     * @param repository
     * @param path
     * @param rootedPath
     * @throws SVNException
     * @throws LoaderException
     */
    private void submit(SVNRepository repository, String path,
            String rootedPath) throws SVNException, LoaderException {

        // hack to skip large mp3 files
        // TODO implement something configurable
        if (path.endsWith(".mp3")) {
            logger.warning("skipping " + path);
            return;
        }

        // queue the file for loading
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        repository.getFile(path, revision, null, baos);

        if (null == baos) {
            throw new NullPointerException(path + " is empty");
        }

        LoaderInterface loader = factory.newLoader(baos.toByteArray());
        loader.setRecordPath(rootedPath);
        // TODO implement path-based type lookup - move to a content factory?
        /*
         * if (path.matches("^.+\\.(xml|xsd)")) { logger.finer(path +
         * " is xml"); loader.setFormat(DocumentFormat.XML); } else if (path
         * .matches("^.+\\.(css|html|incl|js|log|sh|tmpl|txt|xqy)$")) {
         * logger.finer(path + " is text");
         * loader.setFormat(DocumentFormat.TEXT); } else { logger.finer(path +
         * " is binary"); loader.setFormat(DocumentFormat.BINARY); }
         */
        pool.submit(loader);
    }

}
