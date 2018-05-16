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
package org.apache.vxquery.runtime.functions.sequence;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.TypedPointables;
import org.apache.vxquery.datamodel.accessors.nodes.NodeTreePointable;
import org.apache.vxquery.datamodel.builders.sequence.SequenceBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;
import org.apache.vxquery.runtime.functions.util.FunctionHelper;

public class OpIntersectScalarEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public OpIntersectScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws HyracksDataException {
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        final SequenceBuilder sb = new SequenceBuilder();

        final NodeTreePointable temp = (NodeTreePointable) NodeTreePointable.FACTORY.createPointable();
        final SequencePointable seqleft = (SequencePointable) SequencePointable.FACTORY.createPointable();
        final SequencePointable seqright = (SequencePointable) SequencePointable.FACTORY.createPointable();
        final TaggedValuePointable tvpleft = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        final TaggedValuePointable tvpright = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        final TypedPointables tpleft = new TypedPointables();
        final TypedPointables tpright = new TypedPointables();

        Map<Integer, TaggedValuePointable> nodes = new HashMap<Integer, TaggedValuePointable>();

        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                try {
                    abvs.reset();
                    sb.reset(abvs);
                    TaggedValuePointable tvp1 = args[0];
                    TaggedValuePointable tvp2 = args[1];

                    if ((tvp1.getTag() != ValueTag.SEQUENCE_TAG && tvp1.getTag() != ValueTag.NODE_TREE_TAG)
                            || (tvp2.getTag() != ValueTag.SEQUENCE_TAG && tvp2.getTag() != ValueTag.NODE_TREE_TAG)) {
                        throw new SystemException(ErrorCode.FORG0006);
                    }

                    // If an operand has one item then it is a node tree
                    // If an operand has more than one item then it is a sequence               

                    // Add items from the left operand into the hash map
                    if (tvp1.getTag() == ValueTag.SEQUENCE_TAG) {
                        tvp1.getValue(seqleft);
                        for (int i = 0; i < seqleft.getEntryCount(); ++i) {
                            seqleft.getEntry(i, tvpleft);
                            if (tvpleft.getTag() != ValueTag.NODE_TREE_TAG) {
                                throw new SystemException(ErrorCode.XPTY0004);
                            }
                            if (addItem(tvpleft, tpleft, nodes)) {
                                byte data[] = tvpleft.getByteArray();
                                tvpleft.getValue(temp);
                                byte data2[] = temp.getByteArray();
                                if (Arrays.equals(data, data2)) {
                                    System.out.println("true");
                                }
                                else {
                                    System.out.println("false");
                                }
                            }
                        }
                    } else {
                        if (!addItem(tvp1, tpleft, nodes)) {

                        }
                    }

                    // Check if node IDs from right operand are in the hash map
                    if (tvp2.getTag() == ValueTag.SEQUENCE_TAG) {
                        tvp2.getValue(seqright);
                        for (int i = 0; i < seqright.getEntryCount(); ++i) {
                            seqright.getEntry(i, tvpright);
                            if (tvpright.getTag() != ValueTag.NODE_TREE_TAG) {
                                throw new SystemException(ErrorCode.XPTY0004);
                            }
                            if (checkItem(tvpright, tpright, nodes)) {
                                sb.addItem(tvpright);
                            }

                        }
                    } else {
                        if (checkItem(tvp2, tpright, nodes)) {
                            sb.addItem(tvp2);
                        }
                    }

                    sb.finish();
                    result.set(abvs);
                } catch (IOException e) {
                    throw new SystemException(ErrorCode.SYSE0001);
                }
            }
        };
    }

    /*
     * Adds item to nodes (hash map)
     * Returns: False if local node id doesn't exist
     *          True if item added successfully
     */
    private boolean addItem(TaggedValuePointable tvp, TypedPointables tp, Map<Integer, TaggedValuePointable> nodes) {
        int nodeId = FunctionHelper.getLocalNodeId(tvp, tp);
        System.out.println("Node ID: " + nodeId);
        if (nodeId == -1) {
            System.out.println(new String(tvp.getByteArray()));
            return false;
        }
        nodes.put(nodeId, tvp);
        return true;
    }

    private boolean checkItem(TaggedValuePointable tvp, TypedPointables tp, Map<Integer, TaggedValuePointable> nodes) {
        int nodeId = FunctionHelper.getLocalNodeId(tvp, tp);
        if (nodeId == -1) {
            return false;
        } else if (nodes.containsKey(nodeId)) {
            return true;
        }
        return false;
    }
}

/*
    $a := <a>5</a> union <a>6</a>
    $b := a
    $a intersect $b
*/