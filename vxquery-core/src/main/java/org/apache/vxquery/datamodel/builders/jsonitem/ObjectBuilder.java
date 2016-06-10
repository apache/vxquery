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
package org.apache.vxquery.datamodel.builders.jsonitem;

import java.io.IOException;

import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.vxquery.datamodel.builders.base.IBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;

public class ObjectBuilder extends AbstractJsonBuilder implements IBuilder {

    @Override
    public int getValueTag() {
        return ValueTag.OBJECT_TAG;
    }

    public void addItem(UTF8StringPointable key, IValueReference value) throws IOException {
        dataArea.getDataOutput().write(key.getByteArray(), key.getStartOffset(), key.getLength());
        dataArea.getDataOutput().write(value.getByteArray(), value.getStartOffset(), value.getLength());
        slots.append(dataArea.getLength());
    }

}
