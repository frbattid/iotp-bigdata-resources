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
package com.telefonica.iot.bigdata.hadoop.mr.jobs.ngsi;

import com.telefonica.iot.bigdata.hadoop.mr.mappers.KeyGenerator;
import com.telefonica.iot.bigdata.utils.NGSIUtils;
import com.telefonica.iot.bigdata.utils.Utils;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.simple.JSONObject;

/**
 *
 * @author frb
 */
public class FromRowToColumn extends Configured implements Tool {
    
    public static class Columnizer extends Reducer<Text, Text, NullWritable, Text> {

        private void initialize(JSONObject result, JSONObject jsonRecord) {
            result.put("recvTime", jsonRecord.get("recvTime"));
            result.put("fiwareServicePath", jsonRecord.get("fiwareServicePath"));
            result.put("entityId", jsonRecord.get("entityId"));
            result.put("entityType", jsonRecord.get("entityType"));
        } // initialize
        
        private void update(JSONObject result, JSONObject jsonRecord) {
            String attrName = (String) jsonRecord.get("attrName");
            result.put(attrName, jsonRecord.get("attrValue"));
            result.put(attrName + "_md", jsonRecord.get("attrMd"));
        } // update

        @Override
        public void reduce(Text key, Iterable<Text> records, Context context) throws IOException, InterruptedException {
            JSONObject result = new JSONObject();
            
            for (Text record : records) {
                JSONObject jsonRecord = Utils.parseRecord(record);
                
                if (result.isEmpty()) {
                    initialize(result, jsonRecord);
                    update(result, jsonRecord);
                } else {
                    update(result, jsonRecord);
                } // if else
            } // for
            
            context.write(NullWritable.get(), new Text(result.toJSONString()));
        } // reduce
        
    } // Columnizer
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new FromRowToColumn(), args);
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
        conf.set("KEY_FIELDS", "recvTimeTs&fiwareServicePath&entityId&entityType");
        Job job = Job.getInstance(conf, "FromRowToColumn");
        job.setJarByClass(FromRowToColumn.class);
        job.setMapperClass(KeyGenerator.class);
        //job.setCombinerClass(RecordsCombiner.class);
        job.setReducerClass(Columnizer.class);
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
        System.out.println("   com.telefonica.iot.bigdata.hadoop.mr.jobs.ngsi.FromRowToColumn \\");
        System.out.println("   <HDFS input dir> \\");
        System.out.println("   <HDFS output dir>");
    } // showUsage
    
} // FromRowToColumn
