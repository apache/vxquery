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
import edu.uci.ics.hyracks.algebricks.runtime.base.IAggregateEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyAggregateFunction;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyAggregateFunctionFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyRunningAggregateFunctionFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopySerializableAggregateFunctionFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyUnnestingFunction;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyUnnestingFunctionFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.IUnnestingEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IUnnestingEvaluatorFactory;
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
        IAggregateEvaluatorFactory aef = erp.createAggregateFunctionFactory(expr, env, inputSchemas, context);
        return new AggregateCopyEvaluatorFactoryAdapter(aef);
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
        IUnnestingEvaluatorFactory uef = erp.createUnnestingFunctionFactory(expr, env, inputSchemas, context);
        return new UnnestCopyEvaluatorFactoryAdapter(uef);
    }

    private static final class ScalarCopyEvaluatorFactoryAdapter implements ICopyEvaluatorFactory {
        private static final long serialVersionUID = 1L;

        private final IScalarEvaluatorFactory sef;

        public ScalarCopyEvaluatorFactoryAdapter(IScalarEvaluatorFactory sef) {
            this.sef = sef;
        }

        @Override
        public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
            final IScalarEvaluator se = sef.createScalarEvaluator();
            final IPointable p = VoidPointable.FACTORY.createPointable();
            return new ICopyEvaluator() {
                @Override
                public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                    se.evaluate(tuple, p);
                    try {
                        output.getDataOutput().write(p.getByteArray(), p.getStartOffset(), p.getLength());
                    } catch (IOException ex) {
                        throw new AlgebricksException(ex);
                    }
                }
            };
        }
    }

    private static final class AggregateCopyEvaluatorFactoryAdapter implements ICopyAggregateFunctionFactory {
        private static final long serialVersionUID = 1L;

        private final IAggregateEvaluatorFactory aef;

        public AggregateCopyEvaluatorFactoryAdapter(IAggregateEvaluatorFactory aef) {
            this.aef = aef;
        }

        @Override
        public ICopyAggregateFunction createAggregateFunction(final IDataOutputProvider output)
                throws AlgebricksException {
            final IAggregateEvaluator ae = aef.createAggregateEvaluator();
            final IPointable p = VoidPointable.FACTORY.createPointable();
            return new ICopyAggregateFunction() {

                @Override
                public void step(IFrameTupleReference tuple) throws AlgebricksException {
                    ae.step(tuple);
                }

                @Override
                public void init() throws AlgebricksException {
                    ae.init();
                }

                @Override
                public void finishPartial() throws AlgebricksException {
                    finish();
                }

                @Override
                public void finish() throws AlgebricksException {
                    ae.finish(p);
                    try {
                        output.getDataOutput().write(p.getByteArray(), p.getStartOffset(), p.getLength());
                    } catch (IOException ex) {
                        throw new AlgebricksException(ex);
                    }
                }
            };
        }
    }

    private static final class UnnestCopyEvaluatorFactoryAdapter implements ICopyUnnestingFunctionFactory {
        private static final long serialVersionUID = 1L;

        private final IUnnestingEvaluatorFactory uef;

        public UnnestCopyEvaluatorFactoryAdapter(IUnnestingEvaluatorFactory uef) {
            this.uef = uef;
        }

        @Override
        public ICopyUnnestingFunction createUnnestingFunction(final IDataOutputProvider output)
                throws AlgebricksException {
            final IUnnestingEvaluator ue = uef.createUnnestingEvaluator();
            final IPointable p = VoidPointable.FACTORY.createPointable();
            return new ICopyUnnestingFunction() {
                @Override
                public boolean step() throws AlgebricksException {
                    boolean status = ue.step(p);
                    if (status) {
                        try {
                            output.getDataOutput().write(p.getByteArray(), p.getStartOffset(), p.getLength());
                        } catch (IOException ex) {
                            throw new AlgebricksException(ex);
                        }
                        return true;
                    }
                    return false;
                }

                @Override
                public void init(IFrameTupleReference tuple) throws AlgebricksException {
                    ue.init(tuple);
                }
            };
        }
    }
}