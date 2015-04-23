
/**
 * Copyright 2015 Immuta Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.immuta.accumulo;

import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.tserver.logger.LogEvents;
import org.apache.accumulo.tserver.logger.LogFileKey;
import org.apache.accumulo.tserver.logger.LogFileValue;

import org.apache.hadoop.io.Text;

public class TableLogEventHandler implements WriteAheadLogEventHandler {
    private Text tableId;
    private Set<Integer> tids;

    public TableLogEventHandler(String instanceName, String zookeeper, String tableName) {
        this.tids = new HashSet<Integer>();
        try {
            ZooKeeperInstance instance = new ZooKeeperInstance(instanceName, zookeeper);
            this.tableId = new Text(Tables.getTableId(instance, tableName));
        } catch(TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void handleEvent(LogFileKey key, LogFileValue value) {
        if(key.event == LogEvents.DEFINE_TABLET) {
            if(key.tablet.getTableId().equals(tableId)) {
                System.out.println("Adding Tid: " + key.tid);
                tids.add(key.tid);
            }
        } else if(key.event == LogEvents.MUTATION || key.event == LogEvents.MANY_MUTATIONS) {
            if(tids.contains(key.tid)) {
                System.out.println(LogFileValue.format(value, 10));
            }
        }  
    }
}
