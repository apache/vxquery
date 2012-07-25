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

public class FnSubstringBeforeEvaluatorFactory extends AbstractCharacterIteratorCopyingEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnSubstringBeforeEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {
        final UTF8StringPointable stringp1 = new UTF8StringPointable();
        final UTF8StringPointable stringp2 = new UTF8StringPointable();
        final UTF8StringPointable stringp3 = new UTF8StringPointable();
        final SubstringBeforeCharacterIterator stringIterator = new SubstringBeforeCharacterIterator(
                new UTF8StringCharacterIterator(stringp1));
        final UTF8StringCharacterIterator searchIterator = new UTF8StringCharacterIterator(stringp2);

        return new AbstractCharacterIteratorCopyingEvaluator(args, stringIterator) {
            @Override
            protected void preEvaluate(TaggedValuePointable[] args) throws SystemException {
                // Only accept strings as input.
                TaggedValuePointable tvp1 = args[0];
                TaggedValuePointable tvp2 = args[1];

                if (tvp1.getTag() != ValueTag.XS_STRING_TAG) {
                    throw new SystemException(ErrorCode.FORG0006);
                }
                if (tvp2.getTag() != ValueTag.XS_STRING_TAG) {
                    throw new SystemException(ErrorCode.FORG0006);
                }
                tvp1.getValue(stringp1);
                tvp2.getValue(stringp2);
                stringIterator.reset();
                searchIterator.reset();

                // Third parameter is optional.
                if (args.length > 2) {
                    TaggedValuePointable tvp3 = args[2];
                    if (tvp3.getTag() != ValueTag.XS_STRING_TAG) {
                        throw new SystemException(ErrorCode.FORG0006);
                    }
                    tvp3.getValue(stringp3);
                }

                stringIterator.setSearch(searchIterator);
            }
        };
    }
}
