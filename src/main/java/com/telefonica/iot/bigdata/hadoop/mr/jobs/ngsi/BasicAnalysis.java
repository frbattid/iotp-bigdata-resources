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
public class BasicAnalysis extends Configured implements Tool {
    
    public static class Analyzer extends Reducer<Text, Text, NullWritable, Text> {
        
        private String attrsFormat;
        
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            attrsFormat = context.getConfiguration().get("ATTRS_FORMAT", "");
        } // setup
        
        private void initializeAnalysis(JSONObject analysis, Object attrValue) {
            if (attrValue instanceof String) {
                JSONObject counts = new JSONObject();
                counts.put(attrValue, 1);
                analysis.put("counts", counts);
            } else if (attrValue instanceof Long) {
                analysis.put("max", (Long) attrValue);
                analysis.put("min", (Long) attrValue);
                analysis.put("sum", attrValue);
                analysis.put("sum2", ((Long) attrValue * (Long) attrValue));
            } else if (attrValue instanceof Double) {
                analysis.put("max", (Double) attrValue);
                analysis.put("min", (Double) attrValue);
                analysis.put("sum", attrValue);
                analysis.put("sum2", ((Double) attrValue * (Double) attrValue));
            } else if (attrValue instanceof Boolean) {
                analysis.put("and", (Boolean) attrValue);
                analysis.put("or", (Boolean) attrValue);
                JSONObject counts = new JSONObject();
                counts.put(attrValue, 1);
                analysis.put("counts", counts);
            } // if else
        } // initializeAnalysis
        
        private void updateAnalysis(JSONObject analysis, Object attrValue) {
            if (attrValue instanceof String) {
                JSONObject counts = (JSONObject) analysis.get("counts");

                if (counts.containsKey(attrValue)) {
                    counts.put(attrValue, (Integer) counts.get(attrValue) + 1);
                } else {
                    counts.put(attrValue, 1);
                } // if else
            } else if (attrValue instanceof Long) {
                if ((Long) attrValue > (Long) analysis.get("max")) {
                    analysis.put("max", attrValue);
                } // if

                if ((Long) attrValue < (Long) analysis.get("min")) {
                    analysis.put("min", attrValue);
                } // if

                analysis.put("sum", (Long) analysis.get("sum") + (Long) attrValue);
                analysis.put("sum2", (Long) analysis.get("sum2") + ((Long) attrValue * (Long) attrValue));
            } else if (attrValue instanceof Double) {
                if ((Double) attrValue > (Double) analysis.get("max")) {
                    analysis.put("max", attrValue);
                } // if

                if ((Double) attrValue < (Double) analysis.get("min")) {
                    analysis.put("min", attrValue);
                } // if

                analysis.put("sum", (Double) analysis.get("sum") + (Double) attrValue);
                analysis.put("sum2", (Double) analysis.get("sum2") + ((Double) attrValue * (Double) attrValue));
            } else if (attrValue instanceof Boolean) {
                analysis.put("and", (Boolean) analysis.get("and") && (Boolean) attrValue);
                analysis.put("or", (Boolean) analysis.get("or") || (Boolean) attrValue);

                JSONObject counts = (JSONObject) analysis.get("counts");

                if (counts.containsKey(attrValue)) {
                    counts.put(attrValue, (Integer) counts.get(attrValue) + 1);
                } else {
                    counts.put(attrValue, 1);
                } // if else
            } // if else
        } // updateAnalysis
        
        private void analyzeRowFormat(JSONObject jsonRecord, JSONObject jsonAnalysis) {
            Object attrValue = jsonRecord.get("attrValue");
            
            if (jsonAnalysis.isEmpty()) {
                JSONObject analysis = new JSONObject();
                analysis.put("numRecords", 1);
                jsonAnalysis.put("fiwareServicePath", jsonRecord.get("fiwareServicePath"));
                jsonAnalysis.put("entityId", jsonRecord.get("entityId"));
                jsonAnalysis.put("entityType", jsonRecord.get("entityType"));
                jsonAnalysis.put("attrName", jsonRecord.get("attrName"));
                jsonAnalysis.put("attrType", jsonRecord.get("attrType"));
                jsonAnalysis.put("analysis", analysis);
                initializeAnalysis(analysis, attrValue);
            } else {
                JSONObject analysis = (JSONObject) jsonAnalysis.get("analysis");
                analysis.put("numRecords", (Integer) analysis.get("numRecords") + 1);
                updateAnalysis(analysis, attrValue);
            } // if else
        } // analyzeRowFormat
        
        private boolean isAttrValue(String field) {
            return !field.equals("recvTime") && !field.equals("recvTimeTs") && !field.equals("fiwareServicePath")
                    && !field.equals("entityId") && !field.equals("entityType") && !field.contains("_md");
        } // isAttrValue
        
        private void analyzeColumnFormat(JSONObject jsonRecord, JSONObject jsonAnalysis) {
            if (jsonAnalysis.isEmpty()) {
                JSONObject analysis = new JSONObject();
                analysis.put("numRecords", 1);
                jsonAnalysis.put("fiwareServicePath", jsonRecord.get("fiwareServicePath"));
                jsonAnalysis.put("entityId", jsonRecord.get("entityId"));
                jsonAnalysis.put("entityType", jsonRecord.get("entityType"));
                jsonAnalysis.put("analysis", analysis);
                
                Iterator it = jsonRecord.keySet().iterator();

                while (it.hasNext()) {
                    String field = (String) it.next();
                    
                    if (isAttrValue(field)) {
                        Object attrValue = (Object) jsonRecord.get(field);
                        JSONObject fieldAnalysis = (JSONObject) analysis.get(field);
                        
                        if (fieldAnalysis == null) {
                            fieldAnalysis = new JSONObject();
                            analysis.put(field, fieldAnalysis);
                        } // if
                        
                        initializeAnalysis(fieldAnalysis, attrValue);
                    } // if
                } // while
            } else {
                JSONObject analysis = (JSONObject) jsonAnalysis.get("analysis");
                analysis.put("numRecords", (Integer) analysis.get("numRecords") + 1);
                
                Iterator it = jsonRecord.keySet().iterator();

                while (it.hasNext()) {
                    String field = (String) it.next();
                    
                    if (isAttrValue(field)) {
                        Object attrValue = (Object) jsonRecord.get(field);
                        JSONObject fieldAnalysis = (JSONObject) analysis.get(field);
                        
                        if (fieldAnalysis == null) {
                            fieldAnalysis = new JSONObject();
                            analysis.put(field, fieldAnalysis);
                        } // if
                        
                        updateAnalysis(fieldAnalysis, attrValue);
                    } // if
                } // while
            } // if else
        } // analyzeColumnFormat

        @Override
        public void reduce(Text key, Iterable<Text> records, Context context) throws IOException, InterruptedException {
            JSONObject jsonAnalysis = new JSONObject();
            
            for (Text record : records) {
                JSONObject jsonRecord = Utils.parseRecord(record);

                switch (attrsFormat) {
                    case "row":
                        analyzeRowFormat(jsonRecord, jsonAnalysis);
                        break;
                    case "column":
                        analyzeColumnFormat(jsonRecord, jsonAnalysis);
                        break;
                    default:
                        return;
                } // switch
            } // for
            
            context.write(NullWritable.get(), new Text(jsonAnalysis.toJSONString()));
        } // reduce
        
    } // Analyzer
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new BasicAnalysis(), args);
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
        String attrsFormat = args[2];
        
        Configuration conf = new Configuration();
        conf.addResource(new Path("/etc/hadoop/2.3.6.0-3796/0/core-site.xml"));
        conf.addResource(new Path("/etc/hadoop/2.3.6.0-3796/0/hdfs-site.xml"));
        conf.set("ATTRS_FORMAT", attrsFormat);
        
        if (attrsFormat.equals("row")) {
            conf.set("KEY_FIELDS", "fiwareServicePath&entityId&entityType&attrName&attrType");
        } else {
            conf.set("KEY_FIELDS", "fiwareServicePath&entityId&entityType");
        } // if else
        
        Job job = Job.getInstance(conf, "BasicAnalysis");
        job.setJarByClass(BasicAnalysis.class);
        job.setMapperClass(KeyGenerator.class);
        //job.setCombinerClass(RecordsCombiner.class);
        job.setReducerClass(Analyzer.class);
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
        System.out.println("   com.telefonica.iot.bigdata.hadoop.mr.jobs.ngsi.BasicAnalysis \\");
        System.out.println("   <HDFS input dir> \\");
        System.out.println("   <HDFS output dir> \\");
        System.out.println("   <attrs format>");
    } // showUsage
    
} // BasicAnalysis
