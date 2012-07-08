package org.apache.vxquery.runtime.functions.strings;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;

public class FnContainsEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnContainsEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {
        final UTF8StringPointable stringp1 = new UTF8StringPointable();
        final UTF8StringPointable stringp2 = new UTF8StringPointable();
        final UTF8StringPointable stringp3 = new UTF8StringPointable();
        final UTF8StringCharacterIterator charIterator1 = new UTF8StringCharacterIterator(stringp1);
        final UTF8StringCharacterIterator charIterator2 = new UTF8StringCharacterIterator(stringp2);

        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                // Default result is false.
                byte[] booleanResult = new byte[2];
                booleanResult[0] = ValueTag.XS_BOOLEAN_TAG;
                booleanResult[1] = 0;

                TaggedValuePointable tvp1 = args[0];
                TaggedValuePointable tvp2 = args[1];

                // Only accept strings as input.
                if (tvp1.getTag() != ValueTag.XS_STRING_TAG) {
                    throw new SystemException(ErrorCode.FORG0006);
                }
                if (tvp2.getTag() != ValueTag.XS_STRING_TAG) {
                    throw new SystemException(ErrorCode.FORG0006);
                }
                tvp1.getValue(stringp1);
                tvp2.getValue(stringp2);
                charIterator1.reset();
                charIterator2.reset();

                // Third parameter is optional.
                if (args.length > 2) {
                    TaggedValuePointable tvp3 = args[2];
                    if (tvp3.getTag() != ValueTag.XS_STRING_TAG) {
                        throw new SystemException(ErrorCode.FORG0006);
                    }
                    tvp3.getValue(stringp3);
                }
                // TODO use the third value as collation

                // Only need to run comparisons if they both have a non empty string.
                if (stringp1.getLength() > 2 && stringp2.getLength() > 2) {
                    int c2 = charIterator2.next();
                    while (true) {
                        int c1 = charIterator1.next();
                        if (c1 == c2) {
                            int offset1 = charIterator1.getByteOffset();

                            // Check substring.
                            if (checkSubString(charIterator1, charIterator2)) {
                                booleanResult[1] = 1;
                                break;
                            }

                            // Reset for strings for continuation.
                            charIterator2.reset();
                            c2 = charIterator2.next();
                            charIterator1.setByteOffset(offset1);
                        }
                        if (c1 == ICharacterIterator.EOS_CHAR) {
                            // End of string and no match found.
                            break;
                        }
                    }
                } else if (stringp2.getLength() == 2) {
                    booleanResult[1] = 1;
                }

                result.set(booleanResult, 0, 2);
            }

            private boolean checkSubString(ICharacterIterator charIterator1, ICharacterIterator charIterator2) {
                while (true) {
                    int c1 = charIterator1.next();
                    int c2 = charIterator2.next();
                    if (c2 == ICharacterIterator.EOS_CHAR) {
                        // End of string.
                        return true;
                    }
                    if (c1 != c2) {
                        // No match found.
                        break;
                    }
                }
                return false;
            }
        };
    }
}
