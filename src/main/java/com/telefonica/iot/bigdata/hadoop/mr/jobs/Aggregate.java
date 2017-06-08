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
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author frb
 */
public class Aggregate extends Configured implements Tool {
    
    public static class Aggregator extends Reducer<Text, Text, NullWritable, Text> {
        
        private final HashMap<String, String> aggrCriteria = new HashMap<>();
        private final HashMap<String, Object> currValues = new HashMap<>();
        
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            String aggrCriteriaStr = context.getConfiguration().get("AGGR_CRITERIA", "");
            String[] aggrCriteriaStrSplit = aggrCriteriaStr.split("&");
            
            for (String singleAggrCriteriaStr : aggrCriteriaStrSplit) {
                String[] singleAggrCriteriaStrSplit = singleAggrCriteriaStr.split(":");
                
                if (singleAggrCriteriaStrSplit.length == 2) {
                    aggrCriteria.put(singleAggrCriteriaStrSplit[0], singleAggrCriteriaStrSplit[1]);
                } // if
            } // for
        } // setup
        
        private String getAggrCriteria(String field) {
            String defaultAggrCriteria = aggrCriteria.get("_default_");
            String singleAggrCriteria = aggrCriteria.get(field);

            if (singleAggrCriteria == null) {
                if (defaultAggrCriteria == null) {
                    return null;
                } // if

                return defaultAggrCriteria;
            } else {
                return singleAggrCriteria;
            } // if else
        } // getAggrCriteria
        
        private void aggreagate(String condOp, String field, Object currVal, Object val) {
            if (condOp == null) {
                return;
            } // if
            
            switch (condOp) {  
                case "last":
                    //currVal = val;
                    currValues.put(field, val);
                    break;
                case "first":
                    break;
                case "sum":
                    //currVal = (Object) (((Float) currVal) + ((Float) val));
                    currValues.put(field, (Object) (((Float) currVal) + ((Float) val)));
                    break;
                case "max":
                    if (((Float) val) > ((Float) currVal)) {
                        //currVal = val;
                        currValues.put(field, val);
                    } // if
                    
                    break;
                case "min":
                    if (((Float) val) < ((Float) currVal)) {
                        //currVal = val;
                        currValues.put(field, val);
                    } // if
                    
                    break;
                case "and":
                    //currVal = (Object) (((Boolean) currVal) && ((Boolean) val));
                    currValues.put(field, (Object) (((Boolean) currVal) && ((Boolean) val)));
                    break;
                case "or":
                    //currVal = (Object) (((Boolean) currVal) || ((Boolean) val));
                    currValues.put(field, (Object) (((Boolean) currVal) || ((Boolean) val)));
                    break;
            } // switch
        } // aggreagate
        
        private String getPrintableValue(Object val) {
            if (val instanceof String) {
                return "\"" + (String) val + "\"";
            } else if (val instanceof JSONValue) {
                return (String) val;
            } else if (val instanceof JSONObject) {
                return ((JSONObject) val).toJSONString();
            } else if (val instanceof JSONArray) {
                return ((JSONArray) val).toJSONString();
            } else {
                return null;
            } // if else
        } // getPrintableValue
        
        private Text createAggr() {
            String s = "{";
            
            for (String field : currValues.keySet()) {
                Object val = currValues.get(field);
                
                if (s.equals("{")) {
                    s += "\"" + field + "\":" + getPrintableValue(val);
                } else {
                    s += ",\"" + field + "\":" + getPrintableValue(val);
                } // if else
            } // for
            
            s += "}";
            return new Text(s);
        } // createAggr

        @Override
        public void reduce(Text key, Iterable<Text> records, Context context) throws IOException, InterruptedException {
            for (Text record : records) {
                JSONParser parser = new JSONParser();
                JSONObject jsonObject;

                try {
                    jsonObject = (JSONObject) parser.parse(record.toString());
                } catch (ParseException e) {
                    throw new InterruptedException(e.getMessage());
                } // try catch

                Iterator it = jsonObject.keySet().iterator();

                while (it.hasNext()) {
                    String field = (String) it.next();
                    Object val = (Object) jsonObject.get(field);
                    Object currValue = currValues.get(field);
                    
                    if (currValue == null) {
                        currValues.put(field, val);
                    } else {
                        aggreagate(getAggrCriteria(field), field, currValue, val);
                    } // if else
                } // while
            } // for

            context.write(NullWritable.get(), createAggr());
        } // reduce
        
    } // Aggregator
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Aggregate(), args);
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
        String aggrCriteria = args[2];
        
        Configuration conf = new Configuration();
        conf.addResource(new Path("/etc/hadoop/2.3.6.0-3796/0/core-site.xml"));
        conf.addResource(new Path("/etc/hadoop/2.3.6.0-3796/0/hdfs-site.xml"));
        conf.set("AGGR_CRITERIA", aggrCriteria);
        Job job = Job.getInstance(conf, "Aggregate");
        job.setJarByClass(Aggregate.class);
        job.setMapperClass(IdentityMapper.class);
        job.setCombinerClass(RecordsCombiner.class);
        job.setReducerClass(Aggregator.class);
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
        System.out.println("   com.telefonica.iot.bigdata.hadoop.mr.Aggregate \\");
        System.out.println("   <HDFS input dir> \\");
        System.out.println("   <HDFS output dir> \\");
        System.out.println("   <aggregation criteria>");
    } // showUsage
    
} // Aggregate
