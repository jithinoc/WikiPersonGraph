package com.qburst.wikiPersonGraph.mapreduce;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WPGMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text out = new Text();

    public String getTitle(String content) {
        String title = StringUtils.substringBetween(content, "<title>", "</title>");
        if(title == null)
            return null;
        return title.toLowerCase().replaceAll(" ", "_");
    }

    public String[] getLinks(String  content) {
        String[] links;
        links = StringUtils.substringsBetween(content, "[[", "]]");
        return links;
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String content = value.toString();
        String title = this.getTitle(content);
        Text textTitle = new Text(title);
        String[] links = this.getLinks(content);

        if(links != null) {
            for (String link : links) {
                link = link.toLowerCase();
                link = link.trim();
                link = link.replaceAll(" ", "_");
                link = link.split("|")[0];
                out.set(link);
                context.write(out, textTitle);
            }
        }

        out.set(title);
        context.write(out, new Text("**person**"));
    }
}
