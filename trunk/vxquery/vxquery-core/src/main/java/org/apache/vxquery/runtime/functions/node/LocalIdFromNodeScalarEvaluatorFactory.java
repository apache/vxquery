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
package org.apache.vxquery.runtime.functions.node;

import java.io.DataOutput;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.nodes.NodeTreePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;
import org.apache.vxquery.runtime.functions.util.FunctionHelper;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class LocalIdFromNodeScalarEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public LocalIdFromNodeScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        final DataOutput dOut = abvs.getDataOutput();
        final TaggedValuePointable tvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        final FunctionHelper.TypedPointables tp = new FunctionHelper.TypedPointables();

        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                TaggedValuePointable tvp1 = args[0];
                // Only accept node trees as input.
                if (tvp1.getTag() == ValueTag.NODE_TREE_TAG) {
                    try {
                        abvs.reset();
                        tvp1.getValue(tp.ntp);
                        dOut.write(ValueTag.XS_INT_TAG);
                        tp.ntp.getRootNode(tvp);
                        switch (tvp.getTag()) {
                            case ValueTag.ATTRIBUTE_NODE_TAG:
                                tvp.getValue(tp.anp);
                                dOut.writeInt(tp.anp.getLocalNodeId(tp.ntp));
                                break;
                            case ValueTag.COMMENT_NODE_TAG:
                            case ValueTag.TEXT_NODE_TAG:
                                tvp.getValue(tp.tocnp);
                                dOut.writeInt(tp.tocnp.getLocalNodeId(tp.ntp));
                                break;
                            case ValueTag.DOCUMENT_NODE_TAG:
                                tvp.getValue(tp.dnp);
                                dOut.writeInt(tp.dnp.getLocalNodeId(tp.ntp));
                                break;
                            case ValueTag.ELEMENT_NODE_TAG:
                                tvp.getValue(tp.enp);
                                dOut.writeInt(tp.enp.getLocalNodeId(tp.ntp));
                                break;
                            case ValueTag.PI_NODE_TAG:
                                tvp.getValue(tp.pinp);
                                dOut.writeInt(tp.pinp.getLocalNodeId(tp.ntp));
                                break;
                            default:
                                dOut.writeInt(-1);
                        }

                        result.set(abvs);
                    } catch (Exception e) {
                        throw new SystemException(ErrorCode.SYSE0001, e);
                    }
                } else {
                    throw new SystemException(ErrorCode.FORG0006);
                }
            }

        };
    }

}
