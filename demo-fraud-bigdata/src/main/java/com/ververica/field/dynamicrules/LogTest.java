package com.ververica.field.dynamicrules;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URL;

public class LogTest {
    private static final Logger logger = LogManager.getLogger(LogTest.class);

    public static void main(String[] args) {
        URL log4jConfig = Thread.currentThread().getContextClassLoader().getResource("log4j2.properties");
        System.out.println("Log4j2 Config File Path: " + (log4jConfig != null ? log4jConfig.getPath() : "Not Found"));


        logger.info("Log4j2 is working!");
    }
}

