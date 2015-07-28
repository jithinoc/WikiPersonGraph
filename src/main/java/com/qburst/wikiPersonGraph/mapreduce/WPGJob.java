package com.qburst.wikiPersonGraph.mapreduce;

import com.qburst.wikiPersonGraph.helpers.WikiPageInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

public class WPGJob {
    private Logger LOGGER = Logger.getLogger(WPGJob.class);
    private Job job;

    public WPGJob(Configuration configuration) {
        this.job = createJob(configuration);
    }

    public Job createJob(Configuration configuration) {
        Job job=null;
        try {
            job = new Job(configuration, configuration.get("job"));
            job.setJarByClass(WPGJob.class);
            job.setMapperClass(WPGMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setInputFormatClass(WikiPageInputFormat.class);
            TableMapReduceUtil.initTableReducerJob(
                    configuration.get("table"),
                    WPGReducer.class,
                    job
            );
            FileInputFormat.addInputPath(job, new Path(configuration.get("source")));
        }catch (IOException ex) {
            LOGGER.info("Job creation failed");
            ex.printStackTrace();
        }
        return job;
    }
    public boolean run() {
        try {
            return job.waitForCompletion(true);
        }catch (Exception ex) {
            LOGGER.info("Job run failed");
            ex.printStackTrace();
            return false;
        }
    }
}
