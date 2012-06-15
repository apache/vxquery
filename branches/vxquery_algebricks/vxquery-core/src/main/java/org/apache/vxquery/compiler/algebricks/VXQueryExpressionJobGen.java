package org.apache.vxquery.compiler.algebricks;

import java.io.IOException;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionRuntimeProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ILogicalExpressionJobGen;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.StatefulFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyAggregateFunctionFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyRunningAggregateFunctionFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopySerializableAggregateFunctionFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyUnnestingFunctionFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.VoidPointable;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class VXQueryExpressionJobGen implements ILogicalExpressionJobGen {
    private final IExpressionRuntimeProvider erp;

    public VXQueryExpressionJobGen() {
        erp = new VXQueryExpressionRuntimeProvider();
    }

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(ILogicalExpression expr, IVariableTypeEnvironment env,
            IOperatorSchema[] inputSchemas, JobGenContext context) throws AlgebricksException {
        IScalarEvaluatorFactory ef = erp.createEvaluatorFactory(expr, env, inputSchemas, context);
        return new ScalarCopyEvaluatorFactoryAdapter(ef);
    }

    @Override
    public ICopyAggregateFunctionFactory createAggregateFunctionFactory(AggregateFunctionCallExpression expr,
            IVariableTypeEnvironment env, IOperatorSchema[] inputSchemas, JobGenContext context)
            throws AlgebricksException {
        return null;
    }

    @Override
    public ICopySerializableAggregateFunctionFactory createSerializableAggregateFunctionFactory(
            AggregateFunctionCallExpression expr, IVariableTypeEnvironment env, IOperatorSchema[] inputSchemas,
            JobGenContext context) throws AlgebricksException {
        return null;
    }

    @Override
    public ICopyRunningAggregateFunctionFactory createRunningAggregateFunctionFactory(
            StatefulFunctionCallExpression expr, IVariableTypeEnvironment env, IOperatorSchema[] inputSchemas,
            JobGenContext context) throws AlgebricksException {
        return null;
    }

    @Override
    public ICopyUnnestingFunctionFactory createUnnestingFunctionFactory(UnnestingFunctionCallExpression expr,
            IVariableTypeEnvironment env, IOperatorSchema[] inputSchemas, JobGenContext context)
            throws AlgebricksException {
        return null;
    }

    private static final class ScalarCopyEvaluatorFactoryAdapter implements ICopyEvaluatorFactory {
        private static final long serialVersionUID = 1L;

        private final IScalarEvaluatorFactory ef;

        public ScalarCopyEvaluatorFactoryAdapter(IScalarEvaluatorFactory ef) {
            this.ef = ef;
        }

        @Override
        public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
            final IScalarEvaluator e = ef.createScalarEvaluator();
            final IPointable p = VoidPointable.FACTORY.createPointable();
            return new ICopyEvaluator() {
                @Override
                public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                    e.evaluate(tuple, p);
                    try {
                        output.getDataOutput().write(p.getByteArray(), p.getStartOffset(), p.getLength());
                    } catch (IOException ex) {
                        throw new AlgebricksException(ex);
                    }
                }
            };
        }
    }
}