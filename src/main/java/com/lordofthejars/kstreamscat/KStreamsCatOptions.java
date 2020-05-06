package com.lordofthejars.kstreamscat;

import picocli.CommandLine.Option;

public class KStreamsCatOptions {
    
    @Option(names = {"-t", "--topic"}, required = true, paramLabel = "topic", description =  "the topic to interact")
    String topic;

    @Option(names = {"-b"}, paramLabel = "server", description =  "the kafka broker URL", defaultValue = "localhost:9092")
    String broker;

    @Option(names = {"-o"}, paramLabel = "offset", description =  "Offset to start consuming.", defaultValue = "earliest")
    String offset;

    @Option(names = {"--id"}, required = true, paramLabel = "id", description =  "application id")
    String appId;

    @Option(names = {"--GT"}, description = "Creates a Global KTable")
    boolean globalKTable;

    @Option(names = "--time-window", description = "Sets time window in seconds")
    long timeWindow;

    @Option(names = "--session-window", description = "Sets session window in seconds")
    long sessionWindow;

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "display this help message")
    boolean help;
    
}