package com.qburst.wikiPersonGraph;

import com.qburst.wikiPersonGraph.helpers.JobHelper;
import com.qburst.wikiPersonGraph.mapreduce.WPGJob;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class WPGDriver {
    public static void main(String[] args) throws IOException {
        /* args[0] -> path to config.yml */
        Configuration configuration = JobHelper.createConfiguration(args[0]);
        WPGJob wpgJob = new WPGJob(configuration);
        wpgJob.run();
    }
}
