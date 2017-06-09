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

import com.telefonica.iot.bigdata.utils.NGSIUtils;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
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
public class PearsonCorrelation extends Configured implements Tool {

    public static class PairedKeyValueGenerator extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            JSONParser parser = new JSONParser();
            JSONObject jsonObject;

            try {
                jsonObject = (JSONObject) parser.parse(value.toString());
            } catch (ParseException e) {
                throw new InterruptedException(e.getMessage());
            } // try catch

            Iterator it1 = jsonObject.keySet().iterator();

            while (it1.hasNext()) {
                String field1 = (String) it1.next();
                Object val1 = jsonObject.get(field1);

                if (NGSIUtils.isAttrValue(field1) && (val1 instanceof Long || val1 instanceof Double)) {
                    Iterator it2 = jsonObject.keySet().iterator();

                    while (it2.hasNext()) {
                        String field2 = (String) it2.next();
                        Object val2 = jsonObject.get(field2);

                        if (NGSIUtils.isAttrValue(field2) && !field2.equals(field1)
                                && (val2 instanceof Long || val2 instanceof Double)) {
                            String k = jsonObject.get("fiwareServicePath") + "xffff" + jsonObject.get("entityId")
                                    + "xffff" + jsonObject.get("entityType") + "xffff" + field1 + "xffff" + field2;
                            String v = String.valueOf(val1) + "xffff" + String.valueOf(val2);
                            context.write(new Text(k), new Text(v));
                        } // if
                    } // while
                } // if
            } // while
        } // map

    } // PairedKeyValueGenerator

    public static class Correlator extends Reducer<Text, Text, NullWritable, Text> {

        /**
         * Gets Pearson correlation. According to https://en.wikipedia.org/wiki/Pearson_correlation_coefficient
         *
         * @param xxSum
         * @param yySum
         * @param xySum
         * @param xSum
         * @param ySum
         * @param n
         * @return Pearson correlation
         */
        private double pearsonCorrelation(double xxSum, double yySum, double xySum, double xSum, double ySum,
                double n) {
            double num = (n * xySum) - (xSum * ySum);
            double den1 = Math.sqrt((n * xxSum) - Math.pow(xSum, 2.0d));
            double den2 = Math.sqrt((n * yySum) - Math.pow(ySum, 2.0d));
            return num / den1 * den2;
        } // pearsonCorrelation

        /**
         * Gets Pearson correlation. According to http://davidmlane.com/hyperstat/A51911.html
         *
         * @param xxSum
         * @param yySum
         * @param xySum
         * @param xSum
         * @param ySum
         * @param n
         * @return Pearson correlation
         */
        private double pearsonCorrelation2(double xxSum, double yySum, double xySum, double xSum, double ySum,
                double n) {
            double num = xySum - ((xSum * ySum) / n);
            double den1 = xxSum - (Math.pow(xSum, 2.0d) / n);
            double den2 = yySum - (Math.pow(ySum, 2.0d) / n);
            return num / Math.sqrt(den1 * den2);
        } // pearsonCorrelation2

        @Override
        public void reduce(Text key, Iterable<Text> records, Context context) throws IOException, InterruptedException {
            JSONObject jsonAnalysis = new JSONObject();
            double xxSum = 0.0d;
            double yySum = 0.0d;
            double xySum = 0.0d;
            double xSum = 0.0d;
            double ySum = 0.0d;
            double n = 0.0d;

            for (Text record : records) {

                String[] keyParts = key.toString().split("xffff");

                if (jsonAnalysis.isEmpty()) {
                    jsonAnalysis.put("fiwareServicePath", keyParts[0]);
                    jsonAnalysis.put("entityId", keyParts[1]);
                    jsonAnalysis.put("entityType", keyParts[2]);
                    JSONObject analysis = new JSONObject();
                    analysis.put("x", keyParts[3]);
                    analysis.put("y", keyParts[4]);
                    jsonAnalysis.put("analysis", analysis);
                } // if

                String[] recordParts = record.toString().split("xffff");
                double x = new Double(recordParts[0]);
                double y = new Double(recordParts[1]);
                xxSum += Math.pow(x, 2.0d);
                yySum += Math.pow(y, 2.0d);
                xySum += (x * y);
                xSum += x;
                ySum += y;
                n++;
            } // for

            JSONObject analysis = (JSONObject) jsonAnalysis.get("analysis");
            analysis.put("numRecords", n);
            analysis.put("pearsonCorrelation", pearsonCorrelation2(xxSum, yySum, xySum, xSum, ySum, n));
            context.write(NullWritable.get(), new Text(jsonAnalysis.toJSONString()));
        } // reduce

    } // Analyzer

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PearsonCorrelation(), args);
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
        String tmpOutput = "tmp";

        if (attrsFormat.equals("row")) {
            String[] args2 = {input, tmpOutput};
            FromRowToColumn.main(args2);
        } // if

        Configuration conf = new Configuration();
        conf.addResource(new Path("/etc/hadoop/2.3.6.0-3796/0/core-site.xml"));
        conf.addResource(new Path("/etc/hadoop/2.3.6.0-3796/0/hdfs-site.xml"));
        Job job = Job.getInstance(conf, "PearsonCorrelation");
        job.setJarByClass(PearsonCorrelation.class);
        job.setMapperClass(PairedKeyValueGenerator.class);
        //job.setCombinerClass(RecordsCombiner.class);
        job.setReducerClass(Correlator.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        if (attrsFormat.equals("row")) {
            FileInputFormat.addInputPath(job, new Path(tmpOutput));
        } else {
            FileInputFormat.addInputPath(job, new Path(input));
        } // if else

        FileOutputFormat.setOutputPath(job, new Path(output));
        boolean jobResult = job.waitForCompletion(true);
        
        FileSystem hdfs = FileSystem.get(conf);
        Path tmpOutputPath = new Path(tmpOutput);
        
        if (hdfs.exists(tmpOutputPath)) {
            hdfs.delete(tmpOutputPath, true);
        } // if
        
        return jobResult ? 0 : 1;
    } // run

    private void showUsage() {
        System.out.println("Usage:");
        System.out.println();
        System.out.println("hadoop jar \\");
        System.out.println("   iotp-bigdata-resources-0.1.0.jar \\");
        System.out.println("   com.telefonica.iot.bigdata.hadoop.mr.jobs.ngsi.PearsonCorrelation \\");
        System.out.println("   <HDFS input dir> \\");
        System.out.println("   <HDFS output dir> \\");
        System.out.println("   <attrs format>");
    } // showUsage

} // PearsonCorrelation
