package org.apache.vxquery.runtime.functions.strings;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;

public class FnUpperCaseEvaluatorFactory extends AbstractCharacterIteratorCopyingEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnUpperCaseEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {
        final UTF8StringPointable stringp = new UTF8StringPointable();
        final ICharacterIterator charIterator = new UpperCaseCharacterIterator(new UTF8StringCharacterIterator(stringp));
        return new AbstractCharacterIteratorCopyingEvaluator(args, charIterator) {

            @Override
            protected void preEvaluate(TaggedValuePointable[] args) throws SystemException {
                TaggedValuePointable tvp1 = args[0];

                // Only accept strings as input.
                if (tvp1.getTag() != ValueTag.XS_STRING_TAG) {
                    throw new SystemException(ErrorCode.FORG0006);
                }

                tvp1.getValue(stringp);
            }
        };
    }
}
