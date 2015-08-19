package com.qburst.wikiPersonGraph.helpers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;

public class Indexer {
    private Logger LOGGER = Logger.getLogger(Indexer.class);
    private HashSet<String> index = new HashSet<String>();
    public boolean loadIndexFromFile(String location) {
        LOGGER.info("Loading indexer..");
        BufferedReader br;
        try{
            Path path=new Path(location);
            FileSystem fs = FileSystem.get(new Configuration());
            br=new BufferedReader(new InputStreamReader(fs.open(path)));
            return loadIndex(br);
        }catch(Exception e){
            return false;
        }
    }

    private boolean loadIndex(BufferedReader bufferedReader) throws IOException{
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            index.add(line);
        }
        LOGGER.info(index.size()+ " items indexed");
        return true;
    }

    public int size() {
        return index.size();
    }

    public boolean contains(String pattern) {
        return index.contains(pattern);
    }

}
