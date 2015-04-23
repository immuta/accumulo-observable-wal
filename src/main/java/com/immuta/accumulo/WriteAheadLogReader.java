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

import org.apache.hadoop.fs.Path;


public class WriteAheadLogReader {

    private final List<Path> paths;
    private final VolumeManager volumeManager; 

    public WriteAheadLogReader(List<String> paths) throws IOException {
        this(paths, null);
    }

    // Visible for testing
    public WriteAheadLogReader(List<String> paths, VolumeManager volumeManager) throws IOException {
        this.paths = new ArrayList<Path>(paths.size());    
        for(String path : paths) {
            this.paths.add(new Path(path));
        }
        this.volumeManager = getVolumeManager(volumeManager);
    }

    public void handleWriteAheadLogs(WriteAheadLogEventHandler handler) throws IOException {
        LogFileKey key  = new LogFileKey();
        LogFileValue value = new LogFileValue();

        for(Path path : paths) {
            if(volumeManager.isFile(path)) {
                handleFile(path, handler, key, value);    
            } else {
                handleDirectory(path, handler, key, value);    
            }
        }
    }

    private VolumeManager getVolumeManager(VolumeManager volumeManager) throws IOException {
        if(volumeManager != null) {
            return volumeManager;
        }
        return VolumeManagerImpl.get();
    }

    private void handleFile(Path p, WriteAheadLogEventHandler handler, LogFileKey key, 
        LogFileValue value) throws IOException {
        DFSLoggerInputStreams streams;
        streams = DfsLogger.readHeaderAndReturnStream(volumeManager, p, 
                ServerConfiguration.getSiteConfiguration());

        DataInputStream input = streams.getDecryptingInputStream();
        try {
            while(true) {
                try {
                    key.readFields(input);
                    value.readFields(input);
                } catch(EOFException e) {
                    break;
                }
                handler.handleEvent(key, value);
            }
        } finally {
            input.close();
        }
    }
    
    private void handleDirectory(Path p, WriteAheadLogEventHandler handler, LogFileKey key,
            LogFileValue value) throws IOException {
        MultiReader input = new MultiReader(volumeManager, p);
        while(input.next(key, value)) {
            handler.handleEvent(key, value);
        }
    }

}
