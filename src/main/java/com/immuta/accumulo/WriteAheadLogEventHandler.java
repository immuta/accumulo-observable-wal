package com.immuta.accumulo;

/**
 * Copyright 2015 Immuta Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

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
