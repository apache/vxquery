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

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.functions.Function;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionRuntimeProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.StatefulFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopySerializableAggregateFunctionFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.IRunningAggregateEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.IUnnestingEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.evaluators.ConstantEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.evaluators.TupleFieldEvaluatorFactory;

public class VXQueryExpressionRuntimeProvider implements IExpressionRuntimeProvider {
    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(ILogicalExpression expr, IVariableTypeEnvironment env,
            IOperatorSchema[] inputSchemas, JobGenContext context) throws AlgebricksException {
        switch (expr.getExpressionTag()) {
            case CONSTANT:
                VXQueryConstantValue cv = (VXQueryConstantValue) ((ConstantExpression) expr).getValue();
                return new ConstantEvaluatorFactory(cv.getValue());

            case VARIABLE:
                VariableReferenceExpression vrExpr = (VariableReferenceExpression) expr;
                int tupleFieldIndex = inputSchemas[0].findVariable(vrExpr.getVariableReference());
                return new TupleFieldEvaluatorFactory(tupleFieldIndex);

            case FUNCTION_CALL:
                ScalarFunctionCallExpression fcExpr = (ScalarFunctionCallExpression) expr;
                Function fn = (Function) fcExpr.getFunctionInfo();

                IScalarEvaluatorFactory[] argFactories = createArgumentEvaluatorFactories(env, inputSchemas, context,
                        fcExpr.getArguments());

                try {
                    return fn.createScalarEvaluatorFactory(argFactories);
                } catch (SystemException e) {
                    throw new AlgebricksException(e);
                }
        }
        throw new UnsupportedOperationException("Cannot create runtime for " + expr.getExpressionTag());
    }

    private IScalarEvaluatorFactory[] createArgumentEvaluatorFactories(IVariableTypeEnvironment env,
            IOperatorSchema[] inputSchemas, JobGenContext context, List<Mutable<ILogicalExpression>> args)
            throws AlgebricksException {
        IScalarEvaluatorFactory[] argFactories = new IScalarEvaluatorFactory[args.size()];
        for (int i = 0; i < argFactories.length; ++i) {
            Mutable<ILogicalExpression> arg = args.get(i);
            argFactories[i] = createEvaluatorFactory(arg.getValue(), env, inputSchemas, context);
        }
        return argFactories;
    }

    @Override
    public IAggregateEvaluatorFactory createAggregateFunctionFactory(AggregateFunctionCallExpression expr,
            IVariableTypeEnvironment env, IOperatorSchema[] inputSchemas, JobGenContext context)
            throws AlgebricksException {
        Function fn = (Function) expr.getFunctionInfo();

        IScalarEvaluatorFactory[] argFactories = createArgumentEvaluatorFactories(env, inputSchemas, context,
                expr.getArguments());
        try {
            return fn.createAggregateEvaluatorFactory(argFactories);
        } catch (SystemException e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public ICopySerializableAggregateFunctionFactory createSerializableAggregateFunctionFactory(
            AggregateFunctionCallExpression expr, IVariableTypeEnvironment env, IOperatorSchema[] inputSchemas,
            JobGenContext context) throws AlgebricksException {
        return null;
    }

    @Override
    public IRunningAggregateEvaluatorFactory createRunningAggregateFunctionFactory(StatefulFunctionCallExpression expr,
            IVariableTypeEnvironment env, IOperatorSchema[] inputSchemas, JobGenContext context)
            throws AlgebricksException {
        return null;
    }

    @Override
    public IUnnestingEvaluatorFactory createUnnestingFunctionFactory(UnnestingFunctionCallExpression expr,
            IVariableTypeEnvironment env, IOperatorSchema[] inputSchemas, JobGenContext context)
            throws AlgebricksException {
        Function fn = (Function) expr.getFunctionInfo();

        IScalarEvaluatorFactory[] argFactories = createArgumentEvaluatorFactories(env, inputSchemas, context,
                expr.getArguments());
        try {
            return fn.createUnnestingEvaluatorFactory(argFactories);
        } catch (SystemException e) {
            throw new AlgebricksException(e);
        }
    }
}