# Accumulo Observable Write Ahead Log

## Synopsis
A simple framework for dealing with the Accumulo Write Ahead log.

## Example Usage
To use the framework a user simply creates a class that implements the 
WriteAheadLogEventHandler interface. They then pass this handler over to the
WriteAheadLogReader class which will take care of reading the write ahead log
and calling the handler.


An example of a handler can be found at
src/main/java/com/immuta/accumulo/TableLogEventHandler.java

An example of an application can be found at 
src/main/java/com/immuta/accumulo/App.java

The app can be built using 

```mvn clean install ```

It can be run by using the 'accumulo' binary in the bin directory of your
accumulo install.

```accumulo jar accumulo-observable-wal-0.1-SNAPSHOT.jar <path>```

## Tested Against
This has currently been tested against Accumulo 1.6.0 running against CDH 5.3.2

## Requirements
    + java 1.7 or higher
    + maven 
    + accumulo 1.6.0

## License
Apache 2.0
