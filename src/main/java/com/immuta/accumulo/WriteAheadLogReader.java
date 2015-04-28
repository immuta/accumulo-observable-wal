/**
 *  Copyright 2015 Immuta Inc
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.immuta.accumulo;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.tserver.log.DfsLogger;
import org.apache.accumulo.tserver.log.DfsLogger.DFSLoggerInputStreams;
import org.apache.accumulo.tserver.log.MultiReader;
import org.apache.accumulo.tserver.logger.LogFileKey;
import org.apache.accumulo.tserver.logger.LogFileValue;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

public class WriteAheadLogReader {

    private final List<Path> paths;
    private final VolumeManager volumeManager;
    private final WriteAheadLogEventHandler handler;

    public WriteAheadLogReader(List<String> paths, WriteAheadLogEventHandler handler)
        throws IOException {
        this(paths, handler, null);
    }

    WriteAheadLogReader(List<String> paths, WriteAheadLogEventHandler handler,
            VolumeManager volumeManager) throws IOException {
        this.paths = new ArrayList<Path>(paths.size());
        for(String path : paths) {
            this.paths.add(new Path(path));
        }
        this.handler = handler;
        if(volumeManager == null) {
            this.volumeManager = VolumeManagerImpl.get();
        } else {
            this.volumeManager = volumeManager;
        }
    }

    public void handleWriteAheadLogs() throws IOException {
        LogFileKey key  = new LogFileKey();
        LogFileValue value = new LogFileValue();

        for(Path path : paths) {
            if(volumeManager.isFile(path)) {
                tailFile(path, key, value);
            } else {
                handleDirectory(path, key, value);
            }
        }
    }

    private void handleFile(Path p, LogFileKey key, LogFileValue value) throws IOException {
        DFSLoggerInputStreams streams;
        streams = DfsLogger.readHeaderAndReturnStream(volumeManager, p,
                ServerConfiguration.getSiteConfiguration());

        DataInputStream input = streams.getDecryptingInputStream();
        try {
            readFile(input, key, value);
        } finally {
            input.close();
        }
    }

    private void handleDirectory(Path p, LogFileKey key, LogFileValue value) throws IOException {
        MultiReader input = new MultiReader(volumeManager, p);
        while(input.next(key, value)) {
            handler.handleEvent(key, value);
        }
    }

    private void tailFile(Path p, LogFileKey key, LogFileValue value) throws IOException  {
        long offset = -1024;
        while(true) {
            long fileSize = volumeManager.getFileStatus(p).getLen();
            if(offset >= fileSize ) {
                // We are as big as the file so lets take a nap
                try {
                    // after napping check to see if we are still as big as the file
                    Thread.sleep(5000);
                    continue;
                } catch(InterruptedException e) {
                    break;
                }
            }
            // Fetch the stream
            DFSLoggerInputStreams streams;
            streams = DfsLogger.readHeaderAndReturnStream(volumeManager, p,
                    ServerConfiguration.getSiteConfiguration());
            if(offset < 0) {
                offset = streams.getOriginalInput().getPos();
            } else {
                streams.getOriginalInput().seek(offset);
            }
            DataInputStream input = streams.getDecryptingInputStream();
            readFile(input, key, value);
            offset = streams.getOriginalInput().getPos();
        }
    }

    private void readFile(DataInputStream input, LogFileKey key, LogFileValue value) throws IOException {
        while(true) {
            try {
                key.readFields(input);
                value.readFields(input);
            } catch(EOFException e) {
                break;
            }
            handler.handleEvent(key, value);
        }
    }
}
