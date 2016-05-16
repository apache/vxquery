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
package org.apache.vxquery.compiler.algebricks;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.ValueTag;

import org.apache.hyracks.algebricks.data.IBinaryIntegerInspector;
import org.apache.hyracks.algebricks.data.IBinaryIntegerInspectorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.data.std.primitive.IntegerPointable;

public class VXQueryBinaryIntegerInspectorFactory implements IBinaryIntegerInspectorFactory {
    private static final long serialVersionUID = 1L;

    @Override
    public IBinaryIntegerInspector createBinaryIntegerInspector(IHyracksTaskContext ctx) {
        final TaggedValuePointable tvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        final IntegerPointable ip = (IntegerPointable) IntegerPointable.FACTORY.createPointable();
        return new IBinaryIntegerInspector() {
            @Override
            public int getIntegerValue(byte[] bytes, int offset, int length) {
                tvp.set(bytes, offset, length);
                assert tvp.getTag() == ValueTag.XS_INT_TAG;
                tvp.getValue(ip);
                return ip.getInteger();
            }
        };
    }
}
