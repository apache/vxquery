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
package org.apache.vxquery.compiler.rewriter.rules.util;

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.vxquery.compiler.algebricks.VXQueryConstantValue;
import org.apache.vxquery.context.StaticContext;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.functions.BuiltinFunctions;
import org.apache.vxquery.functions.BuiltinOperators;
import org.apache.vxquery.functions.Function;
import org.apache.vxquery.types.AnyNodeType;
import org.apache.vxquery.types.Quantifier;
import org.apache.vxquery.types.SequenceType;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;

public class ExpressionToolbox {
    public static Mutable<ILogicalExpression> findVariableExpression(Mutable<ILogicalExpression> mutableLe,
            LogicalVariable lv) {
        ILogicalExpression le = mutableLe.getValue();
        if (le.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            VariableReferenceExpression vre = (VariableReferenceExpression) le;
            if (vre.getVariableReference() == lv) {
                return mutableLe;
            }
        } else if (le.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression afce = (AbstractFunctionCallExpression) le;
            for (Mutable<ILogicalExpression> argExp : afce.getArguments()) {
                Mutable<ILogicalExpression> resultLe = findVariableExpression(argExp, lv);
                if (resultLe != null) {
                    return resultLe;
                }
            }
        }
        return null;
    }

    public static Mutable<ILogicalExpression> findVariableExpression(Mutable<ILogicalExpression> mutableLe) {
        ILogicalExpression le = mutableLe.getValue();
        if (le.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            return mutableLe;
        } else if (le.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression afce = (AbstractFunctionCallExpression) le;
            for (Mutable<ILogicalExpression> argExp : afce.getArguments()) {
                Mutable<ILogicalExpression> resultLe = findVariableExpression(argExp);
                if (resultLe != null) {
                    return resultLe;
                }
            }
        }
        return null;
    }

    public static void findVariableExpressions(Mutable<ILogicalExpression> mutableLe,
            List<Mutable<ILogicalExpression>> finds) {
        ILogicalExpression le = mutableLe.getValue();
        if (le.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            finds.add(mutableLe);
        } else if (le.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression afce = (AbstractFunctionCallExpression) le;
            for (Mutable<ILogicalExpression> argExp : afce.getArguments()) {
                findVariableExpressions(argExp, finds);
            }
        }
    }

    public static Mutable<ILogicalExpression> findLastFunctionExpression(Mutable<ILogicalExpression> mutableLe) {
        ILogicalExpression le = mutableLe.getValue();
        if (le.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            return null;
        } else if (le.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression afce = (AbstractFunctionCallExpression) le;
            for (Mutable<ILogicalExpression> argExp : afce.getArguments()) {
                if (argExp.getValue().getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                    return mutableLe;
                }
                Mutable<ILogicalExpression> resultLe = findLastFunctionExpression(argExp);
                if (resultLe != null) {
                    return resultLe;
                }
            }
        }
        return null;
    }

    public static Mutable<ILogicalExpression> findFirstFunctionExpression(Mutable<ILogicalExpression> mutableLe,
            FunctionIdentifier fi) {
        ILogicalExpression le = mutableLe.getValue();
        if (le.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression afce = (AbstractFunctionCallExpression) le;
            if (afce.getFunctionIdentifier().equals(fi)) {
                return mutableLe;
            }
            for (Mutable<ILogicalExpression> argExp : afce.getArguments()) {
                Mutable<ILogicalExpression> resultLe = findFirstFunctionExpression(argExp, fi);
                if (resultLe != null) {
                    return resultLe;
                }
            }
        }
        return null;
    }

    /**
     * Find all functions for a specific expression.
     */
    public static void findAllFunctionExpressions(Mutable<ILogicalExpression> mutableLe, FunctionIdentifier fi,
            List<Mutable<ILogicalExpression>> finds) {
        ILogicalExpression le = mutableLe.getValue();
        if (le.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression afce = (AbstractFunctionCallExpression) le;
            if (afce.getFunctionIdentifier().equals(fi)) {
                finds.add(mutableLe);
            }
            for (Mutable<ILogicalExpression> argExp : afce.getArguments()) {
                findAllFunctionExpressions(argExp, fi, finds);
            }
        }
    }

    public static Function getBuiltIn(Mutable<ILogicalExpression> mutableLe) {
        ILogicalExpression le = mutableLe.getValue();
        if (le.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression afce = (AbstractFunctionCallExpression) le;
            for (Function function : BuiltinFunctions.FUNCTION_COLLECTION) {
                if (function.getFunctionIdentifier().equals(afce.getFunctionIdentifier())) {
                    return function;
                }
            }
            for (Function function : BuiltinOperators.OPERATOR_COLLECTION) {
                if (function.getFunctionIdentifier().equals(afce.getFunctionIdentifier())) {
                    return function;
                }
            }
        }
        return null;
    }

    public static void getConstantAsPointable(ConstantExpression typeExpression, TaggedValuePointable tvp) {
        VXQueryConstantValue treatTypeConstant = (VXQueryConstantValue) typeExpression.getValue();
        tvp.set(treatTypeConstant.getValue(), 0, treatTypeConstant.getValue().length);
    }

    public static int getTypeExpressionTypeArgument(Mutable<ILogicalExpression> searchM) {
        final int ARG_TYPE = 1;
        AbstractFunctionCallExpression searchFunction = (AbstractFunctionCallExpression) searchM.getValue();
        ILogicalExpression argType = searchFunction.getArguments().get(ARG_TYPE).getValue();
        if (argType.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            return -1;
        }
        TaggedValuePointable tvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        ExpressionToolbox.getConstantAsPointable((ConstantExpression) argType, tvp);

        IntegerPointable pTypeCode = (IntegerPointable) IntegerPointable.FACTORY.createPointable();
        tvp.getValue(pTypeCode);
        return pTypeCode.getInteger();
    }

    public static SequenceType getTypeExpressionTypeArgument(Mutable<ILogicalExpression> searchM, StaticContext dCtx) {
        int typeId = getTypeExpressionTypeArgument(searchM);
        if (typeId > 0) {
            return dCtx.lookupSequenceType(typeId);
        } else {
            return null;
        }
    }

    public static SequenceType getOutputSequenceType(Mutable<ILogicalOperator> opRef,
            Mutable<ILogicalExpression> argFirstM, StaticContext dCtx) {
        ILogicalExpression argFirstLe = argFirstM.getValue();
        switch (argFirstLe.getExpressionTag()) {
            case FUNCTION_CALL:
                // Only process defined functions.
                Function function = ExpressionToolbox.getBuiltIn(argFirstM);
                if (function == null) {
                    return null;
                } else if (function.getFunctionIdentifier().equals(BuiltinOperators.CAST.getFunctionIdentifier())) {
                    // Special case since case has multiple type outputs.
                    return ExpressionToolbox.getTypeExpressionTypeArgument(argFirstM, dCtx);
                } else {
                    return function.getSignature().getReturnType();
                }
            case CONSTANT:
                // Consider constant values.
                ConstantExpression constantExpression = (ConstantExpression) argFirstLe;
                VXQueryConstantValue constantValue = (VXQueryConstantValue) constantExpression.getValue();
                return constantValue.getType();
            case VARIABLE:
                VariableReferenceExpression variableRefExp = (VariableReferenceExpression) argFirstLe;
                LogicalVariable variableId = variableRefExp.getVariableReference();
                Mutable<ILogicalOperator> variableProducer = OperatorToolbox.findProducerOf(opRef, variableId);
                if (variableProducer == null) {
                    return null;
                }
                AbstractLogicalOperator variableOp = (AbstractLogicalOperator) variableProducer.getValue();
                switch (variableOp.getOperatorTag()) {
                    case DATASOURCESCAN:
                        return SequenceType.create(AnyNodeType.INSTANCE, Quantifier.QUANT_ONE);
                    case UNNEST:
                        UnnestOperator unnest = (UnnestOperator) variableOp;
                        return getOutputSequenceType(variableProducer, unnest.getExpressionRef(), dCtx);
                    case ASSIGN:
                        AssignOperator assign = (AssignOperator) variableOp;
                        for (int i = 0; i < assign.getVariables().size(); ++i) {
                            if (variableId.equals(assign.getVariables().get(i))) {
                                return getOutputSequenceType(variableProducer, assign.getExpressions().get(i), dCtx);
                            }
                        }
                        return null;
                    default:
                        // TODO Consider support for other operators. i.e. Assign.
                        break;
                }

        }
        return null;
    }

    public static boolean isFunctionExpression(Mutable<ILogicalExpression> mutableLe,
            AbstractFunctionCallExpression afce) {
        ILogicalExpression le = mutableLe.getValue();
        if (le.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression fc = (AbstractFunctionCallExpression) le;
        if (!fc.getFunctionIdentifier().equals(afce)) {
            return false;
        }
        return true;
    }
}
