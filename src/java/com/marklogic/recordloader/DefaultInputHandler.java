/**
 * Copyright (c) 2008 Mark Logic Corporation. All rights reserved.
 */
package com.marklogic.recordloader;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class DefaultInputHandler extends AbstractInputHandler {

    private FileFilter filter;

    private ArrayList<File> xmlFiles = new ArrayList<File>();

    private ArrayList<File> zipFiles = new ArrayList<File>();

    private ArrayList<File> gzFiles = new ArrayList<File>();

    private boolean hadInputs;

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.recordloader.InputHandlerInterface#run()
     */
    public void run() throws LoaderException, FatalException {
        configureInputs();

        logger.fine("zipFiles.size = " + zipFiles.size());
        logger.fine("gzFiles.size = " + gzFiles.size());
        logger.info("xmlFiles.size = " + xmlFiles.size());

        if (zipFiles.size() > 0 || gzFiles.size() > 0
                || xmlFiles.size() > 0) {
            getFactory();
            logger.info("populating queue");
            // queue any zip-entries first
            try {
                handleZipFiles();
                handleGzFiles();
                handleFiles();
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
            if (configuration.getThreadCount() > 1) {
                logger.warning("Will not use multiple threads!");
                // pointless, since there will only be one input anyway
                pool.setCorePoolSize(1);
                pool.setMaximumPoolSize(1);
            }
            // NOTE: cannot use file-based identifiers
            if (configuration.isUseFilenameIds()) {
                logger.warning("Ignoring configured "
                        + Configuration.ID_NAME_KEY + "="
                        + configuration.getIdNodeName()
                        + " for standard input");
                configuration.setUseAutomaticIds();
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
                String inputPattern = configuration.getInputPattern();
                String name = _f.getName();
                return _f.isDirectory()
                        || (_f.isFile() && (name.matches(inputPattern)) || name
                                .endsWith(Configuration.ZIP_SUFFIX));
            }
        };

        handleFiles(xmlFiles);
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
            if (file.getName().endsWith(Configuration.ZIP_SUFFIX)) {
                ArrayList<File> zipList = new ArrayList<File>();
                zipList.add(file);
                handleZipFiles(zipList);
                continue;
            }
            logger.fine("queuing " + canonicalPath);
            pool.submit(factory.newLoader(file));
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
                logger.fine("queuing " + path);
                pool.submit(factory.newLoader(new GZIPInputStream(
                        new FileInputStream(file)), name, path));
            }
        }
    }

    private void handleStandardInput() throws LoaderException,
            SecurityException {
        // use standard input
        logger.info("Reading from standard input...");
        pool.submit(factory.newLoader(System.in));
    }

    /**
     * @throws ZipException
     * @throws IOException
     * @throws LoaderException
     */
    private void handleZipFiles() throws ZipException, IOException,
            LoaderException {
        if (null == zipFiles) {
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
        Iterator<String> stringIter;
        Enumeration<? extends ZipEntry> entries;
        File file;
        ZipFile zipFile;
        ZipEntry ze;
        String zipInputPattern = configuration.getZipInputPattern();
        String inputPattern = configuration.getInputPattern();
        List<String> entryNameList;

        // NOTE this technique will intentionally leak zipfile objects!
        fileIter = zipFiles.iterator();
        int size;
        if (!fileIter.hasNext()) {
            return;
        }

        int fileCount = 0;
        while (fileIter.hasNext()) {
            file = fileIter.next();
            try {
                zipFile = new ZipFile(file);
            } catch (ZipException e) {
                // user-friendly error message
                logger.warning("Error opening " + file.getCanonicalPath()
                        + ": " + e + " " + e.getMessage());
                throw e;
            }
            entries = zipFile.entries();
            size = zipFile.size();
            String canonicalPath = file.getCanonicalPath();
            logger.fine("queuing " + size + " entries from zip file "
                    + canonicalPath);
            int count = 0;
            entryNameList = new ArrayList<String>();
            String zipFileName = zipFile.getName();
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
                    logger.finer("skipping " + entryName);
                    continue;
                }
                // check zipInputPattern
                if (null != zipInputPattern
                        && !entryName.matches(zipInputPattern)) {
                    // skip it
                    logger.finer("skipping " + entryName);
                    continue;
                }
                // to avoid closing zip inputs randomly,
                // we have to "leak" them temporarily
                // tell the monitor about them, for later cleanup
                logger.finest("adding " + entryName + " from "
                        + zipFileName);
                entryNameList.add(entryName);
            }
            logger.fine("queued " + count + " entries from zip file "
                    + canonicalPath);
            if (0 < entryNameList.size()) {
                // tell the monitor to clean up after the open zip entries
                monitor.add(zipFile, zipFileName, entryNameList);
            } else {
                // nothing from this one
                logger.info("no entries queued from " + zipFileName);
                zipFile.close();
            }
            // now queue the entries
            // I don't like doing this twice,
            // but Monitor needs to know about all the entries
            // before we submit the first Loader.
            stringIter = entryNameList.iterator();
            while (stringIter.hasNext()) {
                entryName = stringIter.next();
                ze = zipFile.getEntry(entryName);
                pool.submit(factory.newLoader(zipFile.getInputStream(ze),
                        zipFileName, entryName));
                count++;
                if (0 == count % 1000) {
                    logger.finer("queued " + count
                            + " entries from zip file " + canonicalPath);
                }
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
        String path = configuration.getInputPath();
        if (null != path) {
            hadInputs = true;
            file = new File(path);
            if (checkPath(file)) {
                logger.info("adding " + path);
                xmlFiles.add(file);
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
                xmlFiles.add(file);
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
