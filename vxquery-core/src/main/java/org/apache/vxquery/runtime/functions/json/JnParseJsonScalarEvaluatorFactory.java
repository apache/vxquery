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
package org.apache.vxquery.runtime.functions.json;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.io.IOUtils;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.BooleanPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.jsonitem.ObjectPointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.jsonparser.JSONParser;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;
import org.apache.vxquery.xmlparser.IParser;

public class JnParseJsonScalarEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    public JnParseJsonScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws HyracksDataException {
        final UTF8StringPointable stringp = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        final UTF8StringPointable stringp2 = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        final ObjectPointable op = (ObjectPointable) ObjectPointable.FACTORY.createPointable();
        final BooleanPointable bp = (BooleanPointable) BooleanPointable.FACTORY.createPointable();
        final ByteBufferInputStream bbis = new ByteBufferInputStream();
        final DataInputStream di = new DataInputStream(bbis);
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        final ArrayBackedValueStorage abvs1 = new ArrayBackedValueStorage();

        return new AbstractTaggedValueArgumentScalarEvaluator(args) {

            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                TaggedValuePointable tvp = args[0];
                if (tvp.getTag() != ValueTag.XS_STRING_TAG) {
                    throw new SystemException(ErrorCode.FORG0006);
                }
                tvp.getValue(stringp);
                if (args.length == 1) {
                    callParser();
                    result.set(abvs);
                }
                if (args.length > 1) {
                    TaggedValuePointable tvp1 = args[1];
                    if (tvp1.getTag() != ValueTag.OBJECT_TAG) {
                        throw new SystemException(ErrorCode.FORG0006);
                    }
                    tvp1.getValue(op);
                    TaggedValuePointable tempTvp = ppool.takeOne(TaggedValuePointable.class);
                    try {
                        op.getKeys(abvs1);
                        tvp1.set(abvs1);
                        tvp1.getValue(stringp2);
                        op.getValue(stringp2, tempTvp);
                    } catch (IOException e1) {
                        throw new SystemException(ErrorCode.SYSE0001, e1);
                    }
                    if (tempTvp.getTag() != ValueTag.XS_BOOLEAN_TAG) {
                        throw new SystemException(ErrorCode.JNTY0020);
                    }
                    tempTvp.getValue(bp);
                    if (bp.getBoolean() == true) {
                        callParser();
                        result.set(abvs);
                    } else {
                        int items = callParser();
                        if (items > 1) {
                            throw new SystemException(ErrorCode.JNDY0021);
                        } else {
                            result.set(abvs);
                        }
                    }
                    ppool.giveBack(tempTvp);
                }
            }

            public int callParser() throws SystemException {
                int items = 0;
                try {
                    IParser parser = new JSONParser();
                    String input = stringp.toString();
                    InputStreamReader isr = new InputStreamReader(IOUtils.toInputStream(input));
                    items = parser.parse(new BufferedReader(isr), abvs);
                } catch (IOException e) {
                    throw new SystemException(ErrorCode.FODC0002, e);
                }
                return items;
            }

        };
    }

}
