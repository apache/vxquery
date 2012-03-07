package org.apache.vxquery.compiler.jobgen;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ILogicalExpressionJobGen;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.StatefulFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IAggregateFunctionFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IRunningAggregateFunctionFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.ISerializableAggregateFunctionFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IUnnestingFunctionFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;

public class ExpressionJobGen implements ILogicalExpressionJobGen {
    @Override
    public IAggregateFunctionFactory createAggregateFunctionFactory(AggregateFunctionCallExpression arg0,
            IVariableTypeEnvironment arg1, IOperatorSchema[] arg2, JobGenContext arg3) throws AlgebricksException {
        return null;
    }

    @Override
    public IEvaluatorFactory createEvaluatorFactory(ILogicalExpression arg0, IVariableTypeEnvironment arg1,
            IOperatorSchema[] arg2, JobGenContext arg3) throws AlgebricksException {
        return null;
    }

    @Override
    public IRunningAggregateFunctionFactory createRunningAggregateFunctionFactory(StatefulFunctionCallExpression arg0,
            IVariableTypeEnvironment arg1, IOperatorSchema[] arg2, JobGenContext arg3) throws AlgebricksException {
        return null;
    }

    @Override
    public ISerializableAggregateFunctionFactory createSerializableAggregateFunctionFactory(
            AggregateFunctionCallExpression arg0, IVariableTypeEnvironment arg1, IOperatorSchema[] arg2,
            JobGenContext arg3) throws AlgebricksException {
        return null;
    }

    @Override
    public IUnnestingFunctionFactory createUnnestingFunctionFactory(UnnestingFunctionCallExpression arg0,
            IVariableTypeEnvironment arg1, IOperatorSchema[] arg2, JobGenContext arg3) throws AlgebricksException {
        return null;
    }
}