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

import org.apache.hyracks.algebricks.data.IBinaryBooleanInspector;
import org.apache.hyracks.algebricks.data.IBinaryBooleanInspectorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.data.std.primitive.BooleanPointable;

public class VXQueryBinaryBooleanInspectorFactory implements IBinaryBooleanInspectorFactory {
    private static final long serialVersionUID = 1L;

    @Override
    public IBinaryBooleanInspector createBinaryBooleanInspector(IHyracksTaskContext ctx) {
        final TaggedValuePointable tvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        final BooleanPointable bp = (BooleanPointable) BooleanPointable.FACTORY.createPointable();
        return new IBinaryBooleanInspector() {
            @Override
            public boolean getBooleanValue(byte[] bytes, int offset, int length) {
                tvp.set(bytes, offset, length);
                assert tvp.getTag() == ValueTag.XS_BOOLEAN_TAG;
                tvp.getValue(bp);
                return bp.getBoolean();
            }
        };
    }
}
