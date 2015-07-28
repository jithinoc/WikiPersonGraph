package com.qburst.wikiPersonGraph.helpers;

import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Map;


public class YMLParser {
    @SuppressWarnings("unchecked")
    public Map<String, String> getMap(String ymlPath) throws FileNotFoundException{
        Yaml yaml = new Yaml();
        InputStream inputStream = new FileInputStream(new File(ymlPath));
        return (Map<String, String>)yaml.load(inputStream);
    }
}
