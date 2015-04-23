package com.immuta.accumulo;

import org.apache.accumulo.tserver.logger.LogEvents;
import org.apache.accumulo.tserver.logger.LogFileKey;
import org.apache.accumulo.tserver.logger.LogFileValue;

import org.apache.accumulo.core.data.KeyExtent;


public class SimpleLogEventHandler implements WriteAheadLogEventHandler {

    @Override
    public void handleEvent(LogFileKey key, LogFileValue value) {
        if(key.event == LogEvents.DEFINE_TABLET) {
            System.out.println(key);
        }
    }
}
