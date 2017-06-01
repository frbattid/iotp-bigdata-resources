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
package com.telefonica.iot.bigdata.hadoop.mr.reducers;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author frb
 */
public class RecordsJoiner extends Reducer<Text, Text, NullWritable, Text> {
    
    @Override
    public void reduce(Text key, Iterable<Text> records, Context context) throws IOException, InterruptedException {
        for (Text record : records) {
            context.write(NullWritable.get(), record);
        } // for
    } // reduce
    
} // RecordsJoiner
