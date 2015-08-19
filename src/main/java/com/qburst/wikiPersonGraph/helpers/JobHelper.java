package com.qburst.wikiPersonGraph.helpers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.Set;

public class JobHelper {
    private static Logger LOGGER = Logger.getLogger(JobHelper.class);

    public static Configuration createConfiguration(String configFile) {
        Configuration configuration = HBaseConfiguration.create();
        try {
            YMLParser ymlParser = new YMLParser();
            Map<String, String> map = ymlParser.getMap(configFile);
            Set<String> keys = map.keySet();
            for(String key: keys) {
                configuration.set(key, map.get(key));
            }

        } catch (Exception ex) {
            LOGGER.info("CONFIGURATION CREATION FAILED. SOMETHING WRONG IN Config.yml");
            LOGGER.info(ex.getMessage());
            System.exit(1);
        }
        return configuration;
    }
}
