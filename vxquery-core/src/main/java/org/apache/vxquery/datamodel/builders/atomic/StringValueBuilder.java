/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.vxquery.datamodel.builders.atomic;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hyracks.util.string.UTF8StringWriter;

public class StringValueBuilder {
    private final UTF8StringWriter writer;

    public StringValueBuilder() {
        writer = new UTF8StringWriter();
    }

    public void write(CharSequence string, DataOutput out) throws IOException {
        writer.writeUTF8(string, out);
    }
}
