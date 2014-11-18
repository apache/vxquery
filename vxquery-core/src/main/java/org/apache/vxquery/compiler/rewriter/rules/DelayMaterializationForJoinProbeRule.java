/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.vxquery.compiler.rewriter.rules;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.MaterializeOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.IsomorphismUtilities;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.MaterializePOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.OneToOneExchangePOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.ReplicatePOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class DelayMaterializationForJoinProbeRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.INNERJOIN
                && op.getOperatorTag() != LogicalOperatorTag.LEFTOUTERJOIN) {
            return false;
        }

        AbstractBinaryJoinOperator abjo = (AbstractBinaryJoinOperator) op;
        if (abjo.getInputs().size() != 2) {
            return false;
        }

        Mutable<ILogicalOperator> branchProbeRO = findReplicateOperator(abjo.getInputs().get(0));
        Mutable<ILogicalOperator> branchBuildRO = findReplicateOperator(abjo.getInputs().get(1));
        if (branchBuildRO == null || branchBuildRO == null
                || !IsomorphismUtilities.isOperatorIsomorphic(branchProbeRO.getValue(), branchBuildRO.getValue())) {
            return false;
        }

        // Turn off materialization in replicate operator.
        boolean found = false;
        ReplicateOperator ro = (ReplicateOperator) branchProbeRO.getValue();
        boolean[] outputMaterializationFlags = ro.getOutputMaterializationFlags();
        for (int i = 0; i < outputMaterializationFlags.length; ++i) {
            if (outputMaterializationFlags[i]) {
                found = true;
                outputMaterializationFlags[i] = false;
            }
        }
        if (!found) {
            return false;
        }
        ro.setOutputMaterializationFlags(outputMaterializationFlags);

        // Set up references to one level down
        Mutable<ILogicalOperator> parentOp = abjo.getInputs().get(0);
        Mutable<ILogicalOperator> childOpRef = parentOp;
        parentOp = parentOp.getValue().getInputs().get(0);

        // New plan operators
        AbstractLogicalOperator exchange = new ExchangeOperator();
        exchange.setPhysicalOperator(new OneToOneExchangePOperator());
        exchange.setExecutionMode(ExecutionMode.PARTITIONED);
        MutableObject<ILogicalOperator> exchangeRef = new MutableObject<ILogicalOperator>(exchange);
        exchange.getInputs().add(parentOp);

        MaterializeOperator mop = new 
              MaterializeOperator();
        mop.setPhysicalOperator(new MaterializePOperator(false));
        mop.setExecutionMode(ExecutionMode.PARTITIONED);
        Mutable<ILogicalOperator> mopRef = new MutableObject<ILogicalOperator>(mop);
        mop.getInputs().add(exchangeRef);
        AbstractLogicalOperator childOp = (AbstractLogicalOperator) childOpRef.getValue();
        childOp.getInputs().set(0, mopRef);

//        boolean[] materializeFlag = new boolean[1];
//        materializeFlag[0] = true;
//        ReplicateOperator rop = new ReplicateOperator(1, materializeFlag);
//        rop.setPhysicalOperator(new ReplicatePOperator());
//        rop.setExecutionMode(ExecutionMode.PARTITIONED);
//        Mutable<ILogicalOperator> ropRef = new MutableObject<ILogicalOperator>(rop);
//        rop.getInputs().add(exchangeRef);
//
//        // Output
//        rop.getOutputs().add(childOpRef);
//        AbstractLogicalOperator childOp = (AbstractLogicalOperator) childOpRef.getValue();
//        childOp.getInputs().set(0, ropRef);

        context.computeAndSetTypeEnvironmentForOperator(exchange);
        context.computeAndSetTypeEnvironmentForOperator(mop);
        context.computeAndSetTypeEnvironmentForOperator(childOp);
        return true;
    }

    private Mutable<ILogicalOperator> findReplicateOperator(Mutable<ILogicalOperator> input) {
        AbstractLogicalOperator op = (AbstractLogicalOperator) input.getValue();
        if (op.getOperatorTag() == LogicalOperatorTag.REPLICATE) {
            return input;
        }
        if (op.getInputs().size() == 1) {
            return findReplicateOperator(op.getInputs().get(0));
        }
        return null;
    }

//    private void updateBranchContext(Mutable<ILogicalOperator> input, IOptimizationContext context)
//            throws AlgebricksException {
//        AbstractLogicalOperator op = (AbstractLogicalOperator) input.getValue();
//        if (op.getOperatorTag() != LogicalOperatorTag.REPLICATE) {
//            updateBranchContext(op.getInputs().get(0), context);
//        }
//        op.setExecutionMode(ExecutionMode.PARTITIONED);
//        //      context.invalidateTypeEnvironmentForOperator(op);
//        context.computeAndSetTypeEnvironmentForOperator(op);
//        op.computeOutputTypeEnvironment(context);
//    }

}
