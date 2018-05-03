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

import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.context.DynamicContext;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.TypedPointables;
import org.apache.vxquery.datamodel.accessors.nodes.ElementNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.NodeTreePointable;
import org.apache.vxquery.datamodel.builders.sequence.SequenceBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.datamodel.values.XDMConstants;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;
import org.apache.vxquery.runtime.functions.comparison.AbstractValueComparisonOperation;
import org.apache.vxquery.runtime.functions.comparison.ValueEqComparisonOperation;
import org.apache.vxquery.runtime.functions.comparison.general.AbstractGeneralComparisonScalarEvaluatorFactory;
import org.apache.vxquery.runtime.functions.comparison.general.GeneralEqComparisonScalarEvaluatorFactory;
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
        final DataOutput dOut = abvs.getDataOutput();
        final AbstractValueComparisonOperation aOp = new ValueEqComparisonOperation();
        final DynamicContext dCtx = (DynamicContext) ctx.getJobletContext().getGlobalJobData();
        
        final NodeTreePointable ntp1 = (NodeTreePointable) NodeTreePointable.FACTORY.createPointable();
        final NodeTreePointable ntp2 = (NodeTreePointable) NodeTreePointable.FACTORY.createPointable();
        
        
        final SequencePointable seqleft = (SequencePointable) SequencePointable.FACTORY.createPointable();
        final SequencePointable seqright = (SequencePointable) SequencePointable.FACTORY.createPointable();
        final TaggedValuePointable tvpleft = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        final TaggedValuePointable tvpright = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        final TypedPointables tp1 = new TypedPointables();
        final TypedPointables tp2 = new TypedPointables();
        
        final SequenceBuilder sb = new SequenceBuilder();
        
        Set<Integer> nodeIDs = new HashSet<Integer>();
        
        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
            	try {
            		abvs.reset();
            		sb.reset(abvs);
            		TaggedValuePointable tvp1 = args[0];
                    TaggedValuePointable tvp2 = args[1];
                    
                    // If either operand only has one item in it then the tag is a node_tree_tag
                    // TODO: handle if either operand is empty or only has one item
                    if (tvp1.getTag() != ValueTag.SEQUENCE_TAG) {
                    	System.out.println("Not a Sequence: " + tvp1.getTag());
                    	throw new SystemException(ErrorCode.FORG0006);
                    }
                    if (tvp2.getTag() != ValueTag.SEQUENCE_TAG) {
                    	System.out.println("Not a Sequence: " + tvp2.getTag());
                    	throw new SystemException(ErrorCode.FORG0006);
                    }              
                    tvp1.getValue(seqleft);
                    tvp2.getValue(seqright);
                    
                    // naive implementation
                    int seqllen = seqleft.getEntryCount();
                    int seqrlen = seqright.getEntryCount();
                    for (int i = 0 ; i < seqllen; ++i) {
                    	seqleft.getEntry(i, tvpleft);
                    	if (tvpleft.getTag() != ValueTag.NODE_TREE_TAG) {
                    		throw new SystemException(ErrorCode.XPTY0004);
                    	}                  
                    	int lNodeId = FunctionHelper.getLocalNodeId(tvpleft, tp1);
                    	if (lNodeId == -1) {
                    		//TODO
                    		
                    	}
                    	
                    	for (int j = 0; j < seqrlen; ++j) {
                    		seqright.getEntry(j, tvpright);
                        	if (tvpright.getTag() != ValueTag.NODE_TREE_TAG) {
                        		throw new SystemException(ErrorCode.XPTY0004);
                        	}
                        	int rNodeId = FunctionHelper.getLocalNodeId(tvpright, tp1);
                        	if (rNodeId == -1) {
                        		//TODO
                        	
                        	}
                                               	
                        	if (lNodeId == rNodeId) {
                        		sb.addItem(tvpleft);
                        		break;
                        	}
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
}
