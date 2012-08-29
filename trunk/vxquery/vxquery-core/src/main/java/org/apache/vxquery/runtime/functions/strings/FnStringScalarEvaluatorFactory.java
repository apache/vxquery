package org.apache.vxquery.runtime.functions.strings;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSBinaryPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDatePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDateTimePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDecimalPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDurationPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSQNamePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSTimePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;
import org.apache.vxquery.runtime.functions.cast.CastToStringOperation;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.BooleanPointable;
import edu.uci.ics.hyracks.data.std.primitive.BytePointable;
import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.FloatPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;
import edu.uci.ics.hyracks.data.std.primitive.ShortPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class FnStringScalarEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnStringScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        final DataOutput dOut = abvs.getDataOutput();
        final CastToStringOperation castToString = new CastToStringOperation();
        final TypedPointables tp = new TypedPointables();

        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                TaggedValuePointable tvp1 = args[0];
                try {
                    abvs.reset();
                    switch (tvp1.getTag()) {
                        case ValueTag.XS_ANY_URI_TAG:
                        case ValueTag.XS_STRING_TAG:
                        case ValueTag.XS_UNTYPED_ATOMIC_TAG:
                            tvp1.getValue(tp.utf8sp);
                            castToString.convertAnyURI(tp.utf8sp, dOut);
                            break;
                        case ValueTag.XS_BASE64_BINARY_TAG:
                        case ValueTag.XS_HEX_BINARY_TAG:
                            tvp1.getValue(tp.binaryp);
                            castToString.convertBase64Binary(tp.binaryp, dOut);
                            break;
                        case ValueTag.XS_BOOLEAN_TAG:
                            tvp1.getValue(tp.boolp);
                            castToString.convertBoolean(tp.boolp, dOut);
                            break;
                        case ValueTag.XS_DATETIME_TAG:
                            tvp1.getValue(tp.datetimep);
                            castToString.convertDatetime(tp.datetimep, dOut);
                            break;
                        case ValueTag.XS_DAY_TIME_DURATION_TAG:
                        case ValueTag.XS_INTEGER_TAG:
                        case ValueTag.XS_NON_POSITIVE_INTEGER_TAG:
                        case ValueTag.XS_NEGATIVE_INTEGER_TAG:
                        case ValueTag.XS_LONG_TAG:
                        case ValueTag.XS_NON_NEGATIVE_INTEGER_TAG:
                        case ValueTag.XS_UNSIGNED_LONG_TAG:
                        case ValueTag.XS_POSITIVE_INTEGER_TAG:
                        case ValueTag.XS_UNSIGNED_INT_TAG:
                            tvp1.getValue(tp.longp);
                            castToString.convertDTDuration(tp.longp, dOut);
                            break;
                        case ValueTag.XS_DURATION_TAG:
                            tvp1.getValue(tp.durationp);
                            castToString.convertDuration(tp.durationp, dOut);
                            break;
                        case ValueTag.XS_DATE_TAG:
                        case ValueTag.XS_G_DAY_TAG:
                        case ValueTag.XS_G_MONTH_DAY_TAG:
                        case ValueTag.XS_G_MONTH_TAG:
                        case ValueTag.XS_G_YEAR_MONTH_TAG:
                        case ValueTag.XS_G_YEAR_TAG:
                            tvp1.getValue(tp.datep);
                            castToString.convertDate(tp.datep, dOut);
                            break;
                        case ValueTag.XS_QNAME_TAG:
                            tvp1.getValue(tp.qnamep);
                            castToString.convertQName(tp.qnamep, dOut);
                            break;
                        case ValueTag.XS_TIME_TAG:
                            tvp1.getValue(tp.timep);
                            castToString.convertTime(tp.timep, dOut);
                            break;
                        case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                        case ValueTag.XS_INT_TAG:
                        case ValueTag.XS_UNSIGNED_SHORT_TAG:
                            tvp1.getValue(tp.intp);
                            castToString.convertYMDuration(tp.intp, dOut);
                            break;
                        case ValueTag.XS_DECIMAL_TAG:
                            tvp1.getValue(tp.decp);
                            castToString.convertDecimal(tp.decp, dOut);
                            break;
                        case ValueTag.XS_DOUBLE_TAG:
                            tvp1.getValue(tp.doublep);
                            castToString.convertDouble(tp.doublep, dOut);
                            break;
                        case ValueTag.XS_FLOAT_TAG:
                            tvp1.getValue(tp.floatp);
                            castToString.convertFloat(tp.floatp, dOut);
                            break;
                        case ValueTag.XS_SHORT_TAG:
                        case ValueTag.XS_UNSIGNED_BYTE_TAG:
                            tvp1.getValue(tp.shortp);
                            castToString.convertShort(tp.shortp, dOut);
                            break;
                        case ValueTag.XS_BYTE_TAG:
                            tvp1.getValue(tp.bytep);
                            castToString.convertByte(tp.bytep, dOut);
                            break;
                        default:
                            throw new SystemException(ErrorCode.XPDY0002);
                    }

                    result.set(abvs);
                } catch (IOException e) {
                    throw new SystemException(ErrorCode.SYSE0001, e);
                }
            }

        };
    }

    private static class TypedPointables {
        BooleanPointable boolp = (BooleanPointable) BooleanPointable.FACTORY.createPointable();
        BytePointable bytep = (BytePointable) BytePointable.FACTORY.createPointable();
        DoublePointable doublep = (DoublePointable) DoublePointable.FACTORY.createPointable();
        FloatPointable floatp = (FloatPointable) FloatPointable.FACTORY.createPointable();
        IntegerPointable intp = (IntegerPointable) IntegerPointable.FACTORY.createPointable();
        LongPointable longp = (LongPointable) LongPointable.FACTORY.createPointable();
        ShortPointable shortp = (ShortPointable) ShortPointable.FACTORY.createPointable();
        UTF8StringPointable utf8sp = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        XSBinaryPointable binaryp = (XSBinaryPointable) XSBinaryPointable.FACTORY.createPointable();
        XSDatePointable datep = (XSDatePointable) XSDatePointable.FACTORY.createPointable();
        XSDateTimePointable datetimep = (XSDateTimePointable) XSDateTimePointable.FACTORY.createPointable();
        XSDecimalPointable decp = (XSDecimalPointable) XSDecimalPointable.FACTORY.createPointable();
        XSDurationPointable durationp = (XSDurationPointable) XSDurationPointable.FACTORY.createPointable();
        XSTimePointable timep = (XSTimePointable) XSTimePointable.FACTORY.createPointable();
        XSQNamePointable qnamep = (XSQNamePointable) XSQNamePointable.FACTORY.createPointable();
    }
}
