/**
 * Copyright 2017 Telefonica Investigación y Desarrollo, S.A.U
 *
 * This file is part of BigDataResources.
 *
 * BigDataResources is free software: you can redistribute it and/or modify it under the terms of the GNU Affero
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version.
 * BigDataResources is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with BigDataResources. If not, see
 * http://www.gnu.org/licenses/.
 *
 * For those usages not covered by the GNU Affero General Public License please contact with iot_support at tid dot es
 */
package com.telefonica.iot.bigdata.hadoop.mr.jobs;

import com.telefonica.iot.bigdata.hadoop.mr.combiners.RecordsCombiner;
import com.telefonica.iot.bigdata.hadoop.mr.reducers.RecordsJoiner;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author frb
 */
public class Json2CSV extends Configured implements Tool {
    
    public static class FormatConverter extends Mapper<Object, Text, Text, Text> {
        
        private final Text commonKey = new Text("record");

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            JSONParser parser = new JSONParser();
            JSONObject jsonObject;
            
            try {
                jsonObject = (JSONObject) parser.parse(value.toString());
            } catch (ParseException e) {
                throw new InterruptedException(e.getMessage());
            } // try catch
            
            String record = "";
            Iterator it = jsonObject.keySet().iterator();
            
            while (it.hasNext()) {
                String field = (String) it.next();

                if (record.isEmpty()) {
                    record = (String) jsonObject.get(field);
                } else {
                    record += "," + jsonObject.get(field).toString();
                } // if else
            } // while
            
            context.write(commonKey, new Text(record));
        } // map
        
    } // FormatConverter

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Json2CSV(), args);
        System.exit(res);
    } // main
    
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            showUsage();
            return -1;
        } // if
        
        String input = args[0];
        String output = args[1];
        
        Configuration conf = new Configuration();
        conf.addResource(new Path("/etc/hadoop/2.3.6.0-3796/0/core-site.xml"));
        conf.addResource(new Path("/etc/hadoop/2.3.6.0-3796/0/hdfs-site.xml"));
        Job job = Job.getInstance(conf, "Json2CSV");
        job.setJarByClass(Json2CSV.class);
        job.setMapperClass(FormatConverter.class);
        job.setCombinerClass(RecordsCombiner.class);
        job.setReducerClass(RecordsJoiner.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        return job.waitForCompletion(true) ? 0 : 1;
    } // run
    
    private void showUsage() {
        System.out.println("Usage:");
        System.out.println();
        System.out.println("hadoop jar \\");
        System.out.println("   iotp-bigdata-resources-0.1.0.jar \\");
        System.out.println("   com.telefonica.iot.bigdata.hadoop.mr.jobs.Json2CSV \\");
        System.out.println("   <HDFS input dir> \\");
        System.out.println("   <HDFS output dir>");
    } // showUsage
    
} // Json2CSV
