package org.apache.vxquery.runtime.functions.type;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.types.SequenceType;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.api.IPointable;

public class TreatScalarEvaluatorFactory extends AbstractTypeScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public TreatScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {
        return new AbstractTypeScalarEvaluator(args, ctx) {
            @Override
            protected void evaluate(TaggedValuePointable tvp, IPointable result) {
                result.set(tvp);
            }

            @Override
            protected void setSequenceType(SequenceType sType) {

            }
        };
    }
}