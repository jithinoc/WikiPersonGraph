package com.qburst.wikiPersonGraph;

import com.qburst.wikiPersonGraph.helpers.JobHelper;
import com.qburst.wikiPersonGraph.mapreduce.WPGJob;
import org.apache.hadoop.conf.Configuration;

public class WPGDriver {
    public static void main(String[] args) {
        Configuration configuration = JobHelper.createConfiguration(args[0]);
        WPGJob wpgJob = new WPGJob(configuration);
        wpgJob.run();
    }
}
