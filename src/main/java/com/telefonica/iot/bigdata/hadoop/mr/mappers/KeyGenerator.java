/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.telefonica.iot.bigdata.hadoop.mr.mappers;

import com.telefonica.iot.bigdata.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author frb
 */
public class KeyGenerator extends Mapper<Object, Text, Text, Text> {
    
    private ArrayList<String> keyFields;
        
    @Override
    public void setup(Mapper.Context context) throws IOException, InterruptedException {
        String keyFieldsStr = context.getConfiguration().get("KEY_FIELDS", "");
        keyFields = new ArrayList<>(Arrays.asList(keyFieldsStr.split("&")));
    } // setup

    @Override
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        JSONParser parser = new JSONParser();
        JSONObject jsonObject;

        try {
            jsonObject = (JSONObject) parser.parse(value.toString());
        } catch (ParseException e) {
            throw new InterruptedException(e.getMessage());
        } // try catch

        String k = "";
        Iterator it = jsonObject.keySet().iterator();

        while (it.hasNext()) {
            String field = (String) it.next();
            Object val = jsonObject.get(field);

            if (keyFields.contains(field)) {
                k += Utils.serializeJsonValue(val);
            } // if
        } // while

        context.write(new Text(k), value);
    } // map
    
} // KeyGenerator
