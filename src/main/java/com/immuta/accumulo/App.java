package com.immuta.accumulo;

import java.util.List;
import java.util.Arrays;

public class App {
    public static void main(String [] args) throws Exception {
        List<String> myList = Arrays.asList(args);
        WriteAheadLogReader reader = new WriteAheadLogReader(myList);
        reader.handleWriteAheadLogs(new TableLogEventHandler("demo", "zookeeper:2181", "taylor"));
    }
}
