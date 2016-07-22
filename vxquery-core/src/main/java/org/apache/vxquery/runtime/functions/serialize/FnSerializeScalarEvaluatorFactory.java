/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.vxquery.runtime.functions.serialize;

import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;
import org.apache.vxquery.serializer.XMLSerializer;

public class FnSerializeScalarEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    public FnSerializeScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        final ByteArrayAccessibleOutputStream baaos = new ByteArrayAccessibleOutputStream();
        final PrintStream ps = new PrintStream(baaos);
        final XMLSerializer printer = new XMLSerializer();
        final DataOutput out = abvs.getDataOutput();
        return new AbstractTaggedValueArgumentScalarEvaluator(args) {

            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                baaos.reset();
                TaggedValuePointable tvp = args[0];
                printer.printTaggedValuePointable(ps, tvp);
                abvs.reset();
                try {
                    out.write(ValueTag.XS_STRING_TAG);
                    out.write((byte) ((baaos.size() >>> 8) & 0xFF));
                    out.write((byte) ((baaos.size() >>> 0) & 0xFF));
                    out.write(baaos.getByteArray(), 0, baaos.size());
                } catch (IOException e) {
                    throw new SystemException(ErrorCode.XPTY0004);
                }
                result.set(abvs);
            }

        };
    }

}
