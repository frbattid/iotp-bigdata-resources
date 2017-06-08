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
package com.telefonica.iot.bigdata.hadoop.mr;

import com.telefonica.iot.bigdata.hadoop.mr.combiners.RecordsCombiner;
import com.telefonica.iot.bigdata.hadoop.mr.mappers.IdentityMapper;
import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
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
public class CountByField extends Configured implements Tool {
    
    public static class RecordsCounter extends Reducer<Text, Text, NullWritable, Text> {
        
        private String field;
        
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            field = context.getConfiguration().get("FIELD", "");
        } // setup

        @Override
        public void reduce(Text key, Iterable<Text> records, Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> counts = new HashMap<>();
            counts.put("_rest_", 0);
            
            for (Text record : records) {
                JSONParser parser = new JSONParser();
                JSONObject jsonObject;

                try {
                    jsonObject = (JSONObject) parser.parse(record.toString());
                } catch (ParseException e) {
                    throw new InterruptedException(e.getMessage());
                } // try catch
                
                Object val = jsonObject.get(field);
                
                if (val == null) {
                    counts.put("_rest_", counts.get("_rest_") + 1);
                } else {
                    Integer count = counts.get((String) val); // TBD: what happens is val cannot be casted to String
                    
                    if (count == null) {
                        counts.put((String) val, 1);
                    } else {
                        counts.put((String) val, counts.get((String) val) + 1);
                    } // if else
                } // if else
            } // for
            
            String res = "{";

            for (String k : counts.keySet()) {
                if (res.equals("{")) {
                    res += "\"" + k + "\":" + counts.get(k);
                } else {
                    res += ",\"" + k + "\":" + counts.get(k);
                } // if else
            } // for
            
            context.write(NullWritable.get(), new Text(res + "}"));
        } // reduce
        
    } // RecordsCounter
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CountByField(), args);
        System.exit(res);
    } // main
    
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            showUsage();
            return -1;
        } // if
        
        String input = args[0];
        String output = args[1];
        String field = args[2];
        
        Configuration conf = new Configuration();
        conf.addResource(new Path("/etc/hadoop/2.3.6.0-3796/0/core-site.xml"));
        conf.addResource(new Path("/etc/hadoop/2.3.6.0-3796/0/hdfs-site.xml"));
        conf.set("FIELD", field);
        Job job = Job.getInstance(conf, "CountByField");
        job.setJarByClass(CountByField.class);
        job.setMapperClass(IdentityMapper.class);
        job.setCombinerClass(RecordsCombiner.class);
        job.setReducerClass(RecordsCounter.class);
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
        System.out.println("   com.telefonica.iot.bigdata.hadoop.mr.CountByField \\");
        System.out.println("   <HDFS input dir> \\");
        System.out.println("   <HDFS output dir> \\");
        System.out.println("   <field>");
    } // showUsage
    
} // CountByField
