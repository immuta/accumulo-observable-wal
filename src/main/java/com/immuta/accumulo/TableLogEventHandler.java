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
