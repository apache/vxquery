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

public class FnConcatEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnConcatEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {
        final UTF8StringPointable stringp = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        final ArrayBackedValueStorage abvsInner = new ArrayBackedValueStorage();
        final DataOutput dOutInner = abvsInner.getDataOutput();
        final CastToStringOperation castToString = new CastToStringOperation();
        final TypedPointables tp = new TypedPointables();

        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                abvs.reset();

                try {
                    // Byte Format: Type (1 byte) + String Byte Length (2 bytes) + String.
                    DataOutput out = abvs.getDataOutput();
                    out.write(ValueTag.XS_STRING_TAG);

                    // Default values for the length and update later
                    out.write(0xFF);
                    out.write(0xFF);

                    for (int i = 0; i < args.length; i++) {
                        TaggedValuePointable tvp = args[i];

                        // TODO Update function to support cast to a string from any atomic value.
                        if (tvp.getTag() != ValueTag.XS_STRING_TAG) {

                            try {
                                abvsInner.reset();
                                switch (tvp.getTag()) {
                                    case ValueTag.XS_ANY_URI_TAG:
                                        tvp.getValue(tp.utf8sp);
                                        castToString.convertAnyURI(tp.utf8sp, dOutInner);
                                        break;
                                    case ValueTag.XS_STRING_TAG:
                                        tvp.getValue(tp.utf8sp);
                                        castToString.convertString(tp.utf8sp, dOutInner);
                                        break;
                                    case ValueTag.XS_UNTYPED_ATOMIC_TAG:
                                        tvp.getValue(tp.utf8sp);
                                        castToString.convertUntypedAtomic(tp.utf8sp, dOutInner);
                                        break;
                                    case ValueTag.XS_BASE64_BINARY_TAG:
                                        tvp.getValue(tp.binaryp);
                                        castToString.convertBase64Binary(tp.binaryp, dOutInner);
                                        break;
                                    case ValueTag.XS_HEX_BINARY_TAG:
                                        tvp.getValue(tp.binaryp);
                                        castToString.convertHexBinary(tp.binaryp, dOutInner);
                                        break;
                                    case ValueTag.XS_BOOLEAN_TAG:
                                        tvp.getValue(tp.boolp);
                                        castToString.convertBoolean(tp.boolp, dOutInner);
                                        break;
                                    case ValueTag.XS_DATETIME_TAG:
                                        tvp.getValue(tp.datetimep);
                                        castToString.convertDatetime(tp.datetimep, dOutInner);
                                        break;
                                    case ValueTag.XS_DAY_TIME_DURATION_TAG:
                                        tvp.getValue(tp.longp);
                                        castToString.convertDTDuration(tp.longp, dOutInner);
                                        break;
                                    case ValueTag.XS_INTEGER_TAG:
                                    case ValueTag.XS_NON_POSITIVE_INTEGER_TAG:
                                    case ValueTag.XS_NEGATIVE_INTEGER_TAG:
                                    case ValueTag.XS_LONG_TAG:
                                    case ValueTag.XS_NON_NEGATIVE_INTEGER_TAG:
                                    case ValueTag.XS_UNSIGNED_LONG_TAG:
                                    case ValueTag.XS_POSITIVE_INTEGER_TAG:
                                    case ValueTag.XS_UNSIGNED_INT_TAG:
                                        tvp.getValue(tp.longp);
                                        castToString.convertInteger(tp.longp, dOutInner);
                                        break;
                                    case ValueTag.XS_DURATION_TAG:
                                        tvp.getValue(tp.durationp);
                                        castToString.convertDuration(tp.durationp, dOutInner);
                                        break;
                                    case ValueTag.XS_DATE_TAG:
                                        tvp.getValue(tp.datep);
                                        castToString.convertDate(tp.datep, dOutInner);
                                        break;
                                    case ValueTag.XS_G_DAY_TAG:
                                        tvp.getValue(tp.datep);
                                        castToString.convertGDay(tp.datep, dOutInner);
                                        break;
                                    case ValueTag.XS_G_MONTH_DAY_TAG:
                                        tvp.getValue(tp.datep);
                                        castToString.convertGMonthDay(tp.datep, dOutInner);
                                        break;
                                    case ValueTag.XS_G_MONTH_TAG:
                                        tvp.getValue(tp.datep);
                                        castToString.convertGMonth(tp.datep, dOutInner);
                                        break;
                                    case ValueTag.XS_G_YEAR_MONTH_TAG:
                                        tvp.getValue(tp.datep);
                                        castToString.convertGYearMonth(tp.datep, dOutInner);
                                        break;
                                    case ValueTag.XS_G_YEAR_TAG:
                                        tvp.getValue(tp.datep);
                                        castToString.convertGYear(tp.datep, dOutInner);
                                        break;
                                    case ValueTag.XS_QNAME_TAG:
                                        tvp.getValue(tp.qnamep);
                                        castToString.convertQName(tp.qnamep, dOutInner);
                                        break;
                                    case ValueTag.XS_TIME_TAG:
                                        tvp.getValue(tp.timep);
                                        castToString.convertTime(tp.timep, dOutInner);
                                        break;
                                    case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                                        tvp.getValue(tp.intp);
                                        castToString.convertYMDuration(tp.intp, dOutInner);
                                        break;
                                    case ValueTag.XS_INT_TAG:
                                    case ValueTag.XS_UNSIGNED_SHORT_TAG:
                                        tvp.getValue(tp.intp);
                                        castToString.convertInt(tp.intp, dOutInner);
                                        break;
                                    case ValueTag.XS_DECIMAL_TAG:
                                        tvp.getValue(tp.decp);
                                        castToString.convertDecimal(tp.decp, dOutInner);
                                        break;
                                    case ValueTag.XS_DOUBLE_TAG:
                                        tvp.getValue(tp.doublep);
                                        castToString.convertDouble(tp.doublep, dOutInner);
                                        break;
                                    case ValueTag.XS_FLOAT_TAG:
                                        tvp.getValue(tp.floatp);
                                        castToString.convertFloat(tp.floatp, dOutInner);
                                        break;
                                    case ValueTag.XS_SHORT_TAG:
                                    case ValueTag.XS_UNSIGNED_BYTE_TAG:
                                        tvp.getValue(tp.shortp);
                                        castToString.convertShort(tp.shortp, dOutInner);
                                        break;
                                    case ValueTag.XS_BYTE_TAG:
                                        tvp.getValue(tp.bytep);
                                        castToString.convertByte(tp.bytep, dOutInner);
                                        break;
                                    default:
                                        throw new SystemException(ErrorCode.XPDY0002);
                                }

                                stringp.set(abvsInner.getByteArray(), abvsInner.getStartOffset() + 1,
                                        abvsInner.getLength() - 1);
                            } catch (IOException e) {
                                throw new SystemException(ErrorCode.SYSE0001, e);
                            }
                        } else {
                            tvp.getValue(stringp);
                        }

                        out.write(stringp.getByteArray(), stringp.getStartOffset() + 2, stringp.getUTFLength());
                    }

                    // Update the full length string in the byte array.
                    byte[] stringResult = abvs.getByteArray();
                    stringResult[1] = (byte) (((abvs.getLength() - 3) >>> 8) & 0xFF);
                    stringResult[2] = (byte) (((abvs.getLength() - 3) >>> 0) & 0xFF);

                    result.set(stringResult, 0, abvs.getLength());
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
