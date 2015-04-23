package com.immuta.accumulo;

import org.apache.accumulo.tserver.logger.LogFileKey;
import org.apache.accumulo.tserver.logger.LogFileValue;

public interface WriteAheadLogEventHandler {
    /**
     * Handle a Write-Ahead Log event (LogFileKey and LogFileValue)
     *
     * @throws WALHandlerException if something goes wrong while handling the event
     */
    public void handleEvent(LogFileKey key, LogFileValue value);
}
