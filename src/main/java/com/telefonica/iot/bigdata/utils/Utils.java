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
package com.telefonica.iot.bigdata.utils;

import org.apache.hadoop.io.Text;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author frb
 */
public class Utils {
    
    public static JSONObject parseRecord(Text record) {
        JSONParser parser = new JSONParser();

        try {
            return (JSONObject) parser.parse(record.toString());
        } catch (ParseException e) {
            return null;
        } // try catch
    } // parseRecord
    
    public static String serializeJsonValue(Object value) {
        if (value instanceof String) {
            return (String) value;
        } else if (value instanceof Number) {
            return JSONValue.toJSONString(value);
        } else if (value instanceof JSONArray) {
            return ((JSONArray) value).toJSONString();
        } else if (value instanceof JSONObject) {
            return ((JSONObject) value).toJSONString();
        } else {
            return null;
        } // if else
    } // serializeJsonValue
    
} // Utils
