package com.qburst.wikiPersonGraph.mapreduce;

import com.qburst.wikiPersonGraph.utils.Constants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WPGMapper extends Mapper<Text, Text, Text, Text> {
    public String[] getLinks(Text pageContent) {
        String content = pageContent.toString();
        return StringUtils.substringsBetween(content, "[[", "]]");
    }

    public void map(Text pageTitle, Text pageContent, Context context) throws IOException, InterruptedException{
        Text referenceTitle = new Text();

        /* Get all references to other pages from current page */
        String[] links = this.getLinks(pageContent);

        if(links != null) {
            for (String link : links) {
                link = link.toLowerCase();
                link = link.trim();
                link = link.replaceAll(" ", "_");
                referenceTitle.set(link);
                context.write(referenceTitle, pageTitle);
            }
        }

        context.write(pageTitle, new Text(Constants.Person.PERSON_TAG));
    }
}
