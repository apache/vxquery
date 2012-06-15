package org.apache.vxquery.runtime.functions.type;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.data.std.api.IPointable;

public class PromoteScalarEvaluatorFactory extends AbstractTypeScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public PromoteScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IScalarEvaluator[] args) throws AlgebricksException {
        return new AbstractTypeScalarEvaluator(args) {
            @Override
            protected void evaluate(TaggedValuePointable tvp, IPointable result) {
                result.set(tvp);
            }
        };
    }
}