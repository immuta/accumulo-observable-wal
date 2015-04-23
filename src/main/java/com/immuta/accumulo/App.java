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

import java.util.List;
import java.util.Arrays;

public class App {

    private static final String USAGE = String.format("java -jar %s.jar <instance name> <zookeepers>"
                                                      + "<table name> <WAL path>", 
                                                      App.class.getSimpleName());

    public static void main(String [] args) throws Exception {
        if(args.length < 4) {
            System.err.println(USAGE);
            System.exit(-1);
        }
        String instanceName = args[0];
        String zookeepers = args[1];
        String tableName = args[2];

        List<String> myList = Arrays.asList(Arrays.copyOfRange(args, 3, args.length));
        WriteAheadLogReader reader = new WriteAheadLogReader(myList);
        reader.handleWriteAheadLogs(new TableLogEventHandler(instanceName, zookeepers, tableName));
    }
}
