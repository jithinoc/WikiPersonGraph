package com.qburst.wikiPersonGraph.helpers;

import com.qburst.wikiPersonGraph.utils.Constants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

import java.io.IOException;

public class WikiPageRecordReader extends RecordReader<Text, Text> {
    private byte[] startTag;
    private byte[] endTag;
    private long start;
    private long end;
    private FSDataInputStream fsDataInputStream;
    private DataOutputBuffer dataOutputBuffer = new DataOutputBuffer();
    private Text key = new Text();
    private Text value = new Text();
    private Indexer personIndexer = new Indexer();

    @Override
    public void close() throws IOException {
        fsDataInputStream.close();
    }

    @Override
    public float getProgress() throws IOException {
        return (fsDataInputStream.getPos() - start) / (float) (end - start);
    }

    @Override
    public Text getCurrentKey() throws IOException,
            InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException,
            InterruptedException {
        return value;
    }

    private String getCategory(String content) {
        String category = StringUtils.substringBetween(content, "{{Infobox ", "\n");
        if(category == null) {
            return null;
        }
        return category.toLowerCase();
    }

    private String getPageTitle(String content) {
        String pageTitle = StringUtils.substringBetween(
                content, Constants.Page.OPEN_TITLE_TAG, Constants.Page.CLOSE_TITLE_TAG
        );
        if(pageTitle == null)
            return null;
        return pageTitle.toLowerCase().replaceAll(" ", "_");
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        while(true) {
            if (fsDataInputStream.getPos() < end) {
                if (readUntilMatch(startTag, false)) {
                    try {
                        dataOutputBuffer.write(startTag);
                        if (readUntilMatch(endTag, true)) {
                            String content = Bytes.toString(dataOutputBuffer.getData());
                            String pageTitle = this.getPageTitle(content);
                            String category = this.getCategory(content);
                            if(personIndexer.contains(category) && pageTitle != null) {
                                key.set(pageTitle);
                                value.set(dataOutputBuffer.getData(), 0,
                                        dataOutputBuffer.getLength());
                                return true;
                            }
                        }
                    } finally {
                        dataOutputBuffer.reset();
                    }
                }
            } else {
                return false;
            }
        }
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        personIndexer.loadIndexFromFile(configuration.get("indexfile"));
        startTag = Constants.Page.OPEN_PAGE_TAG.getBytes("utf-8");
        endTag = Constants.Page.CLOSE_PAGE_TAG.getBytes("utf-8");
        FileSplit fileSplit = (FileSplit) split;

        start = fileSplit.getStart();
        end = start + fileSplit.getLength();

        Path file = fileSplit.getPath();
        FileSystem fs = file.getFileSystem(configuration);
        fsDataInputStream = fs.open(fileSplit.getPath());
        fsDataInputStream.seek(start);

    }

    private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
        int i = 0;
        while (true) {
            int character = fsDataInputStream.read();
            if (character == -1)
                return false;
            if (withinBlock)
                dataOutputBuffer.write(character);
            if (character == match[i]) {
                i++;
                if (i >= match.length)
                    return true;
            } else
                i = 0;
            if (!withinBlock && i == 0 && fsDataInputStream.getPos() >= end)
                return false;
        }
    }

}
