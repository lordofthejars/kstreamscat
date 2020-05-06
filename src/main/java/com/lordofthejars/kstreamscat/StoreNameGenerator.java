package com.lordofthejars.kstreamscat;

public class StoreNameGenerator {
    
    public static String generate(KStreamsCatOptions options) {
        return options.topic + "-store";
    }

}