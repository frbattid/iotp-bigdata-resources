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

/**
 *
 * @author frb
 */
public class Count extends Configured implements Tool {
    
    public static class RecordsCounter extends Reducer<Text, Text, NullWritable, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> records, Context context) throws IOException, InterruptedException {
            int count = 0;
            
            for (Text record : records) {
                count++;
            } // for

            context.write(NullWritable.get(), new Text("{\"count\":" + count + "}"));
        } // reduce
        
    } // RecordsCounter
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Count(), args);
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
        Job job = Job.getInstance(conf, "Count");
        job.setJarByClass(Count.class);
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
        System.out.println("   com.telefonica.iot.bigdata.hadoop.mr.Count \\");
        System.out.println("   <HDFS input dir> \\");
        System.out.println("   <HDFS output dir>");
    } // showUsage
    
} // Count
