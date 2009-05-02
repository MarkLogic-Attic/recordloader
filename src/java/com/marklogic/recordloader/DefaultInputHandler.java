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
package com.marklogic.recordloader;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class DefaultInputHandler extends AbstractInputHandler {

    private FileFilter filter;

    private ArrayList<File> plainFiles = new ArrayList<File>();

    private ArrayList<File> zipFiles = new ArrayList<File>();

    private ArrayList<File> gzFiles = new ArrayList<File>();

    private boolean hadInputs;

    private int inputCount = 0;

    private long sizeLimit;

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.recordloader.InputHandlerInterface#run()
     */
    public void run() throws LoaderException, FatalException {
        sizeLimit = config.getFileSizeLimit();

        configureInputs();

        logger.fine("zipFiles.size = " + zipFiles.size());
        logger.fine("gzFiles.size = " + gzFiles.size());
        logger.fine("plainFiles.size = " + plainFiles.size());

        if (zipFiles.size() > 0 || gzFiles.size() > 0
                || plainFiles.size() > 0) {
            getFactory();
            if (config.isFirstLoop()) {
                logger.info("populating queue");
            }
            // queue any zip-entries first
            try {
                handleZipFiles();
                handleGzFiles();
                handleFiles();
                if (config.isFirstLoop()) {
                    logger.info("queued " + inputCount + " loader(s)");
                }
            } catch (ZipException e) {
                throw new LoaderException(e);
            } catch (IOException e) {
                throw new LoaderException(e);
            } finally {
                // cleanup
            }
        } else if (hadInputs) {
            throw new FatalException(
                    "input files specified, but none found");
        } else {
            if (config.getThreadCount() > 1) {
                logger.warning("Will not use multiple threads!");
                // pointless, since there will only be one input anyway
                pool.setCorePoolSize(1);
                pool.setMaximumPoolSize(1);
            }
            // NOTE: cannot use file-based identifiers
            if (config.isUseFilenameIds()) {
                logger.warning("Ignoring configured "
                        + Configuration.ID_NAME_KEY + "="
                        + config.getIdNodeName() + " for standard input");
                config.setUseAutomaticIds();
            }
            getFactory();
            handleStandardInput();
        }

    }

    /**
     * @throws IOException
     * @throws LoaderException
     */
    private void handleFiles() throws IOException, LoaderException {
        filter = new FileFilter() {
            public boolean accept(File _f) {
                String inputPattern = config.getInputPattern();
                String name = _f.getName();
                return _f.isDirectory()
                        || (_f.isFile() && (name.matches(inputPattern)) || name
                                .endsWith(Configuration.ZIP_SUFFIX));
            }
        };

        handleFiles(plainFiles);
    }

    /**
     * @param _files
     * @throws IOException
     * @throws LoaderException
     */
    private void handleFiles(ArrayList<File> _files) throws IOException,
            LoaderException {
        Iterator<File> iter;
        File file;
        String canonicalPath;
        // queue any files, recursing into directories
        iter = _files.iterator();
        while (iter.hasNext()) {
            file = iter.next();
            canonicalPath = file.getCanonicalPath();
            if (file.isDirectory()) {
                logger.fine("directory " + canonicalPath);
                File[] dirList = file.listFiles(filter);
                if (dirList.length > 0) {
                    logger.info("queuing contents of " + canonicalPath
                            + ": " + dirList.length);
                    ArrayList<File> newlist = new ArrayList<File>();
                    for (int i = 0; i < dirList.length; i++) {
                        newlist.add(dirList[i]);
                    }
                    logger.finer("queuing " + newlist.size() + " items");
                    handleFiles(newlist);
                } else {
                    logger.fine("skipping " + canonicalPath
                            + ": no matches");
                }
                continue;
            }
            if (canonicalPath.endsWith(Configuration.ZIP_SUFFIX)) {
                // inefficient, but how many zip files will you queue?
                ArrayList<File> zipList = new ArrayList<File>();
                zipList.add(file);
                handleZipFiles(zipList);
                continue;
            }

            // check size
            if (0 < sizeLimit && file.length() > sizeLimit) {
                logger.info("skipping " + canonicalPath
                        + ": larger than " + sizeLimit + " B");
                continue;
            }

            // plain file - add to the queue
            submit(canonicalPath, factory.newLoader(file));
        }
    }

    /**
     * @throws IOException
     * @throws LoaderException
     */
    private void handleGzFiles() throws IOException, LoaderException {
        if (null == gzFiles) {
            return;
        }

        File file;
        String name;
        String path;
        Iterator<File> iter = gzFiles.iterator();

        if (iter.hasNext()) {
            while (iter.hasNext()) {
                file = iter.next();
                name = file.getName();
                if (name.endsWith(".tar.gz") || name.endsWith(".tgz")) {
                    // assume a tar file
                    logger.warning("skipping unsupported tar file "
                            + file.getCanonicalPath());
                    continue;
                }

                path = file.getPath();
                submit(path, factory.newLoader(new GZIPInputStream(
                        new FileInputStream(file)), name, path));
            }
        }
    }

    private void submit(String _path, LoaderInterface _loader) {
        pool.submit(_loader);
        inputCount++;
        logger.fine("queued " + inputCount + ": " + _path);
    }

    private void handleStandardInput() throws LoaderException,
            SecurityException {
        // use standard input
        logger.info("Reading from standard input...");
        submit("standard input", factory.newLoader(System.in));
    }

    /**
     * @throws ZipException
     * @throws IOException
     * @throws LoaderException
     */
    private void handleZipFiles() throws ZipException, IOException,
            LoaderException {
        if (null == zipFiles || 1 > zipFiles.size()) {
            return;
        }
        handleZipFiles(zipFiles);
    }

    /**
     * @throws ZipException
     * @throws IOException
     * @throws LoaderException
     */
    private void handleZipFiles(ArrayList<File> zipFiles)
            throws ZipException, IOException, LoaderException {

        String entryName;
        Iterator<File> fileIter;
        Enumeration<? extends ZipEntry> entries;
        File file;
        ZipReference zipFile;
        ZipEntry ze;
        String inputPattern = config.getInputPattern();

        fileIter = zipFiles.iterator();
        int size;
        if (!fileIter.hasNext()) {
            return;
        }

        int fileCount = 0;
        while (fileIter.hasNext()) {
            file = fileIter.next();
            try {
                zipFile = new ZipReference(file, logger);
            } catch (ZipException e) {
                // user-friendly error message
                logger.warning("Error opening " + file.getCanonicalPath()
                        + ": " + e + " " + e.getMessage());
                throw e;
            }

            // prevent the zip from closing while we queue
            zipFile.addReference();

            entries = zipFile.entries();
            size = zipFile.size();
            String canonicalPath = file.getCanonicalPath();
            logger.fine("queuing " + size + " entries from zip file "
                    + canonicalPath);
            int count = 0;
            String zipFileName = zipFile.getName();
            // monitor will track the references for us
            // TODO teach the Callable to track the zip references?
            monitor.add(zipFile, zipFileName);

            while (entries.hasMoreElements()) {
                ze = entries.nextElement();
                logger.fine("found zip entry " + ze);
                // getName returns full entry path
                entryName = ze.getName();
                if (ze.isDirectory()) {
                    // skip it
                    logger.finer("skipping directory entry " + entryName);
                    continue;
                }

                // check inputPattern
                if (!entryName.matches(inputPattern)) {
                    // skip it
                    logger.info("skipping " + entryName);
                    continue;
                }

                // to avoid closing zip inputs randomly,
                // we have to "leak" them temporarily
                // via reference counts.
                zipFile.addReference();
                submit(zipFileName + "/" + entryName, factory.newLoader(
                        zipFile.getInputStream(ze), zipFileName,
                        entryName));
                count++;
                if (0 == count % 1000) {
                    logger.finer("queued " + count
                            + " entries from zip file " + canonicalPath);
                }
            }
            logger.fine("queued " + count + " entries from zip file "
                    + canonicalPath);

            zipFile.closeReference();

            if (1 > count) {
                // nothing from this one
                logger.info("no entries queued from " + zipFileName);
                // does not leak - we just closed the last reference
                continue;
            }

            fileCount++;
            if (0 == fileCount % 100) {
                logger.info("queued " + fileCount + " zip files");
            }

        }
    }

    private void configureInputs() {
        File file;

        // handle input-path property, if any
        String path = config.getInputPath();
        if (null != path) {
            hadInputs = true;
            file = new File(path);
            if (checkPath(file)) {
                logger.info("adding " + path);
                plainFiles.add(file);
            }
        }

        if (0 != inputs.length) {
            hadInputs = true;
        }
        for (int i = 0; i < inputs.length; i++) {
            file = new File(inputs[i]);
            if (!checkPath(file)) {
                continue;
            }
            if (inputs[i].endsWith(Configuration.ZIP_SUFFIX)) {
                zipFiles.add(file);
            } else if (inputs[i].endsWith(".gz")) {
                gzFiles.add(file);
            } else {
                plainFiles.add(file);
            }
        }
    }

    /**
     * @param _file
     * @return
     */
    private boolean checkPath(File _file) {
        if (!_file.exists()) {
            logger.warning("skipping " + _file.getPath()
                    + ": file does not exist.");
            return false;
        }
        if (!_file.canRead()) {
            logger.warning("skipping " + _file.getPath()
                    + ": file cannot be read.");
            return false;
        }
        return true;
    }
}
