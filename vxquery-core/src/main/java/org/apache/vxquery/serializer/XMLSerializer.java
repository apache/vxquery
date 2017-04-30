/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.vxquery.serializer;

import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.hyracks.algebricks.data.IPrinter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.BooleanPointable;
import org.apache.hyracks.data.std.primitive.BytePointable;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.ShortPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.accessors.PointablePool;
import org.apache.vxquery.datamodel.accessors.PointablePoolFactory;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.atomic.CodedQNamePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSBinaryPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDatePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDateTimePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDecimalPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDurationPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSQNamePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSTimePointable;
import org.apache.vxquery.datamodel.accessors.jsonitem.ArrayPointable;
import org.apache.vxquery.datamodel.accessors.jsonitem.ObjectPointable;
import org.apache.vxquery.datamodel.accessors.nodes.AttributeNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.DocumentNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.ElementNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.NodeTreePointable;
import org.apache.vxquery.datamodel.accessors.nodes.PINodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.TextOrCommentNodePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.cast.CastToStringOperation;

public class XMLSerializer implements IPrinter {
    private final PointablePool pp;

    private NodeTreePointable ntp;

    private ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
    private DataOutput dOut = abvs.getDataOutput();
    private CastToStringOperation castToString = new CastToStringOperation();

    public XMLSerializer() {
        pp = PointablePoolFactory.INSTANCE.createPointablePool();
    }

    @Override
    public void print(byte[] b, int s, int l, PrintStream ps) {
        TaggedValuePointable tvp = pp.takeOne(TaggedValuePointable.class);
        try {
            tvp.set(b, s, l);
            printTaggedValuePointable(ps, tvp);
        } finally {
            pp.giveBack(tvp);
        }
    }

    public void printTaggedValuePointable(PrintStream ps, TaggedValuePointable tvp) {
        byte tag = tvp.getTag();
        switch ((int) tag) {
            case ValueTag.XS_ANY_URI_TAG:
                printString(ps, tvp);
                break;

            case ValueTag.XS_BASE64_BINARY_TAG:
                printBase64Binary(ps, tvp);
                break;

            case ValueTag.XS_BOOLEAN_TAG:
                printBoolean(ps, tvp);
                break;

            case ValueTag.XS_DATE_TAG:
                printDate(ps, tvp);
                break;

            case ValueTag.XS_DATETIME_TAG:
                printDateTime(ps, tvp);
                break;

            case ValueTag.XS_DAY_TIME_DURATION_TAG:
                printDTDuration(ps, tvp);
                break;

            case ValueTag.XS_BYTE_TAG:
                printByte(ps, tvp);
                break;

            case ValueTag.XS_DECIMAL_TAG:
                printDecimal(ps, tvp);
                break;

            case ValueTag.XS_DOUBLE_TAG:
                printDouble(ps, tvp);
                break;

            case ValueTag.XS_DURATION_TAG:
                printDuration(ps, tvp);
                break;

            case ValueTag.XS_FLOAT_TAG:
                printFloat(ps, tvp);
                break;

            case ValueTag.XS_G_DAY_TAG:
                printGDay(ps, tvp);
                break;

            case ValueTag.XS_G_MONTH_TAG:
                printGMonth(ps, tvp);
                break;

            case ValueTag.XS_G_MONTH_DAY_TAG:
                printGMonthDay(ps, tvp);
                break;

            case ValueTag.XS_G_YEAR_TAG:
                printGYear(ps, tvp);
                break;

            case ValueTag.XS_G_YEAR_MONTH_TAG:
                printGYearMonth(ps, tvp);
                break;

            case ValueTag.XS_HEX_BINARY_TAG:
                printHexBinary(ps, tvp);
                break;

            case ValueTag.XS_INT_TAG:
            case ValueTag.XS_UNSIGNED_SHORT_TAG:
                printInt(ps, tvp);
                break;

            case ValueTag.XS_INTEGER_TAG:
            case ValueTag.XS_LONG_TAG:
            case ValueTag.XS_NEGATIVE_INTEGER_TAG:
            case ValueTag.XS_NON_POSITIVE_INTEGER_TAG:
            case ValueTag.XS_NON_NEGATIVE_INTEGER_TAG:
            case ValueTag.XS_POSITIVE_INTEGER_TAG:
            case ValueTag.XS_UNSIGNED_INT_TAG:
            case ValueTag.XS_UNSIGNED_LONG_TAG:
                printInteger(ps, tvp);
                break;

            case ValueTag.XS_NOTATION_TAG:
                printString(ps, tvp);
                break;

            case ValueTag.XS_QNAME_TAG:
                printQName(ps, tvp);
                break;

            case ValueTag.XS_SHORT_TAG:
            case ValueTag.XS_UNSIGNED_BYTE_TAG:
                printShort(ps, tvp);
                break;

            case ValueTag.XS_STRING_TAG:
            case ValueTag.XS_NORMALIZED_STRING_TAG:
            case ValueTag.XS_TOKEN_TAG:
            case ValueTag.XS_LANGUAGE_TAG:
            case ValueTag.XS_NMTOKEN_TAG:
            case ValueTag.XS_NAME_TAG:
            case ValueTag.XS_NCNAME_TAG:
            case ValueTag.XS_ID_TAG:
            case ValueTag.XS_IDREF_TAG:
            case ValueTag.XS_ENTITY_TAG:
                printString(ps, tvp);
                break;

            case ValueTag.XS_TIME_TAG:
                printTime(ps, tvp);
                break;

            case ValueTag.XS_UNTYPED_ATOMIC_TAG:
                printString(ps, tvp);
                break;

            case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                printYMDuration(ps, tvp);
                break;

            case ValueTag.SEQUENCE_TAG:
                printSequence(ps, tvp);
                break;

            case ValueTag.NODE_TREE_TAG:
                printNodeTree(ps, tvp);
                break;

            case ValueTag.DOCUMENT_NODE_TAG:
                printDocumentNode(ps, tvp);
                break;

            case ValueTag.ELEMENT_NODE_TAG:
                printElementNode(ps, tvp);
                break;

            case ValueTag.ARRAY_TAG:
                printArray(ps, tvp);
                break;

            case ValueTag.ATTRIBUTE_NODE_TAG:
                printAttributeNode(ps, tvp);
                break;

            case ValueTag.TEXT_NODE_TAG:
                printTextNode(ps, tvp);
                break;

            case ValueTag.COMMENT_NODE_TAG:
                printCommentNode(ps, tvp);
                break;

            case ValueTag.PI_NODE_TAG:
                printPINode(ps, tvp);
                break;

            case ValueTag.OBJECT_TAG:
                printObject(ps, tvp);
                break;

            case ValueTag.JS_NULL_TAG:
                printNull(ps, tvp);
                break;
            default:
                throw new UnsupportedOperationException("Encountered tag: " + tvp.getTag());
        }
    }

    private void printNull(PrintStream ps, TaggedValuePointable tvp) {
        ps.append("null");
    }

    private void printDecimal(PrintStream ps, TaggedValuePointable tvp) {
        XSDecimalPointable dp = pp.takeOne(XSDecimalPointable.class);
        try {
            tvp.getValue(dp);
            abvs.reset();
            castToString.convertDecimal(dp, dOut);
            printStringAbvs(ps);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(dp);
        }
    }

    private void printNodeTree(PrintStream ps, TaggedValuePointable tvp) {
        if (ntp != null) {
            throw new IllegalStateException("Nested NodeTreePointable found");
        }
        ntp = pp.takeOne(NodeTreePointable.class);
        TaggedValuePointable rootTVP = pp.takeOne(TaggedValuePointable.class);
        try {
            tvp.getValue(ntp);
            ntp.getRootNode(rootTVP);
            printTaggedValuePointable(ps, rootTVP);
        } finally {
            pp.giveBack(rootTVP);
            pp.giveBack(ntp);
            ntp = null;
        }
    }

    private void printPINode(PrintStream ps, TaggedValuePointable tvp) {
        PINodePointable pnp = pp.takeOne(PINodePointable.class);
        UTF8StringPointable utf8sp = pp.takeOne(UTF8StringPointable.class);
        try {
            tvp.getValue(pnp);
            ps.append("<?");
            pnp.getTarget(ntp, utf8sp);
            printString(ps, utf8sp);
            ps.append(' ');
            pnp.getContent(ntp, utf8sp);
            printString(ps, utf8sp);
            ps.append("?>");
        } finally {
            pp.giveBack(pnp);
            pp.giveBack(utf8sp);
        }
    }

    private void printCommentNode(PrintStream ps, TaggedValuePointable tvp) {
        TextOrCommentNodePointable tcnp = pp.takeOne(TextOrCommentNodePointable.class);
        UTF8StringPointable utf8sp = pp.takeOne(UTF8StringPointable.class);
        try {
            tvp.getValue(tcnp);
            tcnp.getValue(ntp, utf8sp);
            ps.append("<!--");
            printString(ps, utf8sp);
            ps.append("-->");
        } finally {
            pp.giveBack(tcnp);
            pp.giveBack(utf8sp);
        }
    }

    private void printTextNode(PrintStream ps, TaggedValuePointable tvp) {
        TextOrCommentNodePointable tcnp = pp.takeOne(TextOrCommentNodePointable.class);
        UTF8StringPointable utf8sp = pp.takeOne(UTF8StringPointable.class);
        try {
            tvp.getValue(tcnp);
            tcnp.getValue(ntp, utf8sp);
            printString(ps, utf8sp);
        } finally {
            pp.giveBack(tcnp);
            pp.giveBack(utf8sp);
        }
    }

    private void printAttributeNode(PrintStream ps, TaggedValuePointable tvp) {
        AttributeNodePointable anp = pp.takeOne(AttributeNodePointable.class);
        CodedQNamePointable cqp = pp.takeOne(CodedQNamePointable.class);
        UTF8StringPointable utf8sp = pp.takeOne(UTF8StringPointable.class);
        TaggedValuePointable valueTVP = pp.takeOne(TaggedValuePointable.class);
        try {
            tvp.getValue(anp);
            anp.getName(cqp);
            printPrefixedQName(ps, cqp, utf8sp);
            ps.append("=\"");
            anp.getValue(ntp, valueTVP);
            printTaggedValuePointable(ps, valueTVP);
            ps.append('"');
        } finally {
            pp.giveBack(valueTVP);
            pp.giveBack(utf8sp);
            pp.giveBack(anp);
            pp.giveBack(cqp);
        }
    }

    private void printObject(PrintStream ps, TaggedValuePointable tvp) {
        ObjectPointable op = pp.takeOne(ObjectPointable.class);
        TaggedValuePointable keys = pp.takeOne(TaggedValuePointable.class);
        ArrayBackedValueStorage mvs = new ArrayBackedValueStorage();

        tvp.getValue(op);
        try {
            op.getKeys(mvs);
            keys.set(mvs);
            ps.append('{');
            if (keys.getTag() == ValueTag.SEQUENCE_TAG) {
                printObjectPairs(ps, keys, op);
            } else {
                printObjectPair(ps, keys, op);
            }
            ps.append('}');
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(keys);
            pp.giveBack(op);
        }
    }

    private void printObjectPairs(PrintStream ps, TaggedValuePointable keys, ObjectPointable op) {
        SequencePointable seqp = pp.takeOne(SequencePointable.class);
        TaggedValuePointable tvp = pp.takeOne(TaggedValuePointable.class);
        try {
            keys.getValue(seqp);
            int len = seqp.getEntryCount();
            for (int i = 0; i < len; i++) {
                seqp.getEntry(i, tvp);
                printObjectPair(ps, tvp, op);
                if (i != len - 1) {
                    ps.append(',');
                }
            }
        } finally {
            pp.giveBack(seqp);
            pp.giveBack(tvp);
        }
    }

    private void printObjectPair(PrintStream ps, TaggedValuePointable key, ObjectPointable op) {
        UTF8StringPointable utf8sp = pp.takeOne(UTF8StringPointable.class);
        TaggedValuePointable tvp = pp.takeOne(TaggedValuePointable.class);
        try {
            printQuotedTaggedValuePointable(ps, key);
            key.getValue(utf8sp);
            ps.append(":");
            op.getValue(utf8sp, tvp);
            printJsonValue(ps, tvp);
        } finally {
            pp.giveBack(tvp);
            pp.giveBack(utf8sp);
        }
    }

    private void printArray(PrintStream ps, TaggedValuePointable tvp) {
        ArrayPointable ap = pp.takeOne(ArrayPointable.class);
        try {
            tvp.getValue(ap);
            int len = ap.getEntryCount();
            ps.append('[');
            for (int i = 0; i < len; i++) {
                ap.getEntry(i, tvp);
                printJsonValue(ps, tvp);
                if (i != len - 1) {
                    ps.append(',');
                }
            }
            ps.append(']');
        } finally {
            pp.giveBack(ap);
        }
    }

    private void printJsonValue(PrintStream ps, TaggedValuePointable tvp) {
        int tag = tvp.getTag();
        switch (tag) {
            case ValueTag.ARRAY_TAG:
            case ValueTag.ATTRIBUTE_NODE_TAG:
            case ValueTag.COMMENT_NODE_TAG:
            case ValueTag.DOCUMENT_NODE_TAG:
            case ValueTag.ELEMENT_NODE_TAG:
            case ValueTag.JS_NULL_TAG:
            case ValueTag.NODE_TREE_TAG:
            case ValueTag.OBJECT_TAG:
            case ValueTag.PI_NODE_TAG:
            case ValueTag.TEXT_NODE_TAG:
            case ValueTag.XS_BOOLEAN_TAG:
            case ValueTag.XS_BYTE_TAG:
            case ValueTag.XS_DECIMAL_TAG:
            case ValueTag.XS_DOUBLE_TAG:
            case ValueTag.XS_FLOAT_TAG:
            case ValueTag.XS_INT_TAG:
            case ValueTag.XS_INTEGER_TAG:
            case ValueTag.XS_LONG_TAG:
            case ValueTag.XS_NEGATIVE_INTEGER_TAG:
            case ValueTag.XS_NON_NEGATIVE_INTEGER_TAG:
            case ValueTag.XS_NON_POSITIVE_INTEGER_TAG:
            case ValueTag.XS_POSITIVE_INTEGER_TAG:
            case ValueTag.XS_SHORT_TAG:
            case ValueTag.XS_UNSIGNED_BYTE_TAG:
            case ValueTag.XS_UNSIGNED_INT_TAG:
            case ValueTag.XS_UNSIGNED_LONG_TAG:
            case ValueTag.XS_UNSIGNED_SHORT_TAG:
                printTaggedValuePointable(ps, tvp);
                break;
            default:
                printQuotedTaggedValuePointable(ps, tvp);
        }
    }

    private void printQuotedTaggedValuePointable(PrintStream ps, TaggedValuePointable tvp) {
        ps.append('\"');
        printTaggedValuePointable(ps, tvp);
        ps.append('\"');
    }

    private void printElementNode(PrintStream ps, TaggedValuePointable tvp) {
        ElementNodePointable enp = pp.takeOne(ElementNodePointable.class);
        CodedQNamePointable cqp = pp.takeOne(CodedQNamePointable.class);
        UTF8StringPointable utf8sp = pp.takeOne(UTF8StringPointable.class);
        SequencePointable seqp = pp.takeOne(SequencePointable.class);
        try {
            tvp.getValue(enp);
            enp.getName(cqp);
            ps.append('<');
            printPrefixedQName(ps, cqp, utf8sp);

            int nsCount = enp.getNamespaceEntryCount(ntp);
            for (int i = 0; i < nsCount; ++i) {
                ps.append(" xmlns:");
                ntp.getString(enp.getNamespacePrefixCode(ntp, i), utf8sp);
                printString(ps, utf8sp);
                ps.append("=\"");
                ntp.getString(enp.getNamespaceURICode(ntp, i), utf8sp);
                printString(ps, utf8sp);
                ps.append("\"");
            }

            enp.getAttributeSequence(ntp, seqp);
            if (seqp.getByteArray() != null && seqp.getEntryCount() > 0) {
                ps.append(' ');
                printSequence(ps, seqp, " ");
            }

            enp.getChildrenSequence(ntp, seqp);
            if (seqp.getByteArray() != null) {
                ps.append('>');
                printSequence(ps, seqp);
                ps.append("</");
                printPrefixedQName(ps, cqp, utf8sp);
                ps.append('>');
            } else {
                ps.append("/>");
            }
        } finally {
            pp.giveBack(seqp);
            pp.giveBack(utf8sp);
            pp.giveBack(cqp);
            pp.giveBack(enp);
        }
    }

    private void printPrefixedQName(PrintStream ps, CodedQNamePointable cqp, UTF8StringPointable utf8sp) {
        ntp.getString(cqp.getPrefixCode(), utf8sp);
        if (utf8sp.getStringLength() > 0) {
            printString(ps, utf8sp);
            ps.append(':');
        }
        ntp.getString(cqp.getLocalCode(), utf8sp);
        printString(ps, utf8sp);
    }

    private void printDocumentNode(PrintStream ps, TaggedValuePointable tvp) {
        DocumentNodePointable dnp = pp.takeOne(DocumentNodePointable.class);
        SequencePointable seqp = pp.takeOne(SequencePointable.class);
        try {
            ps.append("<?xml version=\"1.0\"?>\n");
            tvp.getValue(dnp);
            dnp.getContent(ntp, seqp);
            printSequence(ps, seqp);
        } finally {
            pp.giveBack(seqp);
            pp.giveBack(dnp);
        }
    }

    private void printSequence(PrintStream ps, TaggedValuePointable tvp) {
        SequencePointable seqp = pp.takeOne(SequencePointable.class);
        try {
            tvp.getValue(seqp);
            printSequence(ps, seqp);
        } finally {
            pp.giveBack(seqp);
        }
    }

    private void printSequence(PrintStream ps, SequencePointable seqp) {
        printSequence(ps, seqp, null);
    }

    private void printSequence(PrintStream ps, SequencePointable seqp, String between) {
        VoidPointable vp = pp.takeOne(VoidPointable.class);
        try {
            int len = seqp.getEntryCount();
            for (int i = 0; i < len; ++i) {
                seqp.getEntry(i, vp);
                print(vp.getByteArray(), vp.getStartOffset(), vp.getLength(), ps);
                if (i < len - 1 && between != null) {
                    ps.append(between);
                }
            }
        } finally {
            pp.giveBack(vp);
        }
    }

    private void printBase64Binary(PrintStream ps, TaggedValuePointable tvp) {
        XSBinaryPointable bp = pp.takeOne(XSBinaryPointable.class);
        try {
            tvp.getValue(bp);
            abvs.reset();
            castToString.convertBase64Binary(bp, dOut);
            printStringAbvs(ps);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(bp);
        }
    }

    private void printBoolean(PrintStream ps, TaggedValuePointable tvp) {
        BooleanPointable bp = pp.takeOne(BooleanPointable.class);
        try {
            tvp.getValue(bp);
            ps.print(bp.getBoolean());
        } finally {
            pp.giveBack(bp);
        }
    }

    private void printByte(PrintStream ps, TaggedValuePointable tvp) {
        BytePointable bp = pp.takeOne(BytePointable.class);
        try {
            tvp.getValue(bp);
            ps.print(bp.byteValue());
        } finally {
            pp.giveBack(bp);
        }
    }

    private void printDouble(PrintStream ps, TaggedValuePointable tvp) {
        DoublePointable dp = pp.takeOne(DoublePointable.class);
        try {
            tvp.getValue(dp);
            abvs.reset();
            castToString.convertDouble(dp, dOut);
            printStringAbvs(ps);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(dp);
        }
    }

    private void printDate(PrintStream ps, TaggedValuePointable tvp) {
        XSDatePointable dp = pp.takeOne(XSDatePointable.class);
        try {
            tvp.getValue(dp);
            abvs.reset();
            castToString.convertDate(dp, dOut);
            printStringAbvs(ps);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(dp);
        }
    }

    private void printDateTime(PrintStream ps, TaggedValuePointable tvp) {
        XSDateTimePointable dtp = pp.takeOne(XSDateTimePointable.class);
        try {
            tvp.getValue(dtp);
            abvs.reset();
            castToString.convertDatetime(dtp, dOut);
            printStringAbvs(ps);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(dtp);
        }
    }

    private void printDTDuration(PrintStream ps, TaggedValuePointable tvp) {
        LongPointable lp = pp.takeOne(LongPointable.class);
        try {
            tvp.getValue(lp);
            abvs.reset();
            castToString.convertDTDuration(lp, dOut);
            printStringAbvs(ps);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(lp);
        }
    }

    private void printDuration(PrintStream ps, TaggedValuePointable tvp) {
        XSDurationPointable dp = pp.takeOne(XSDurationPointable.class);
        try {
            tvp.getValue(dp);
            abvs.reset();
            castToString.convertDuration(dp, dOut);
            printStringAbvs(ps);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(dp);
        }
    }

    private void printFloat(PrintStream ps, TaggedValuePointable tvp) {
        FloatPointable fp = pp.takeOne(FloatPointable.class);
        try {
            tvp.getValue(fp);
            abvs.reset();
            castToString.convertFloat(fp, dOut);
            printStringAbvs(ps);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(fp);
        }
    }

    private void printGDay(PrintStream ps, TaggedValuePointable tvp) {
        XSDatePointable dp = pp.takeOne(XSDatePointable.class);
        try {
            tvp.getValue(dp);
            abvs.reset();
            castToString.convertGDay(dp, dOut);
            printStringAbvs(ps);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(dp);
        }
    }

    private void printGMonth(PrintStream ps, TaggedValuePointable tvp) {
        XSDatePointable dp = pp.takeOne(XSDatePointable.class);
        try {
            tvp.getValue(dp);
            abvs.reset();
            castToString.convertGMonth(dp, dOut);
            printStringAbvs(ps);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(dp);
        }
    }

    private void printGMonthDay(PrintStream ps, TaggedValuePointable tvp) {
        XSDatePointable dp = pp.takeOne(XSDatePointable.class);
        try {
            tvp.getValue(dp);
            abvs.reset();
            castToString.convertGMonthDay(dp, dOut);
            printStringAbvs(ps);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(dp);
        }
    }

    private void printGYear(PrintStream ps, TaggedValuePointable tvp) {
        XSDatePointable dp = pp.takeOne(XSDatePointable.class);
        try {
            tvp.getValue(dp);
            abvs.reset();
            castToString.convertGYear(dp, dOut);
            printStringAbvs(ps);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(dp);
        }
    }

    private void printGYearMonth(PrintStream ps, TaggedValuePointable tvp) {
        XSDatePointable dp = pp.takeOne(XSDatePointable.class);
        try {
            tvp.getValue(dp);
            abvs.reset();
            castToString.convertGYearMonth(dp, dOut);
            printStringAbvs(ps);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(dp);
        }
    }

    private void printHexBinary(PrintStream ps, TaggedValuePointable tvp) {
        XSBinaryPointable bp = pp.takeOne(XSBinaryPointable.class);
        try {
            tvp.getValue(bp);
            abvs.reset();
            castToString.convertHexBinary(bp, dOut);
            printStringAbvs(ps);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(bp);
        }
    }

    private void printInt(PrintStream ps, TaggedValuePointable tvp) {
        IntegerPointable ip = pp.takeOne(IntegerPointable.class);
        try {
            tvp.getValue(ip);
            ps.print(ip.intValue());
        } finally {
            pp.giveBack(ip);
        }
    }

    private void printInteger(PrintStream ps, TaggedValuePointable tvp) {
        LongPointable lp = pp.takeOne(LongPointable.class);
        try {
            tvp.getValue(lp);
            ps.print(lp.longValue());
        } finally {
            pp.giveBack(lp);
        }
    }

    private void printShort(PrintStream ps, TaggedValuePointable tvp) {
        ShortPointable sp = pp.takeOne(ShortPointable.class);
        try {
            tvp.getValue(sp);
            ps.print(sp.shortValue());
        } finally {
            pp.giveBack(sp);
        }
    }

    private void printQName(PrintStream ps, TaggedValuePointable tvp) {
        XSQNamePointable dp = pp.takeOne(XSQNamePointable.class);
        try {
            tvp.getValue(dp);
            abvs.reset();
            castToString.convertQName(dp, dOut);
            printStringAbvs(ps);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(dp);
        }
    }

    private void printStringAbvs(PrintStream ps) {
        UTF8StringPointable utf8sp = pp.takeOne(UTF8StringPointable.class);
        try {
            utf8sp.set(abvs.getByteArray(), abvs.getStartOffset() + 1, abvs.getLength() - 1);
            printString(ps, utf8sp);
        } finally {
            pp.giveBack(utf8sp);
        }
    }

    private void printString(PrintStream ps, TaggedValuePointable tvp) {
        UTF8StringPointable utf8sp = pp.takeOne(UTF8StringPointable.class);
        try {
            tvp.getValue(utf8sp);
            printString(ps, utf8sp);
        } finally {
            pp.giveBack(utf8sp);
        }
    }

    private void printString(PrintStream ps, UTF8StringPointable utf8sp) {
        int utfLen = utf8sp.getUTF8Length();
        int offset = utf8sp.getMetaDataLength();
        while (utfLen > 0) {
            char c = utf8sp.charAt(offset);
            switch (c) {
                case '<':
                    ps.append("&lt;");
                    break;

                case '>':
                    ps.append("&gt;");
                    break;

                case '&':
                    ps.append("&amp;");
                    break;

                case '"':
                    ps.append("&quot;");
                    break;

                case '\'':
                    ps.append("&apos;");
                    break;

                default:
                    ps.append(c);
                    break;
            }
            int cLen = utf8sp.charSize(offset);
            offset += cLen;
            utfLen -= cLen;
        }
    }

    private void printTime(PrintStream ps, TaggedValuePointable tvp) {
        XSTimePointable tp = pp.takeOne(XSTimePointable.class);
        try {
            tvp.getValue(tp);
            abvs.reset();
            castToString.convertTime(tp, dOut);
            printStringAbvs(ps);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(tp);
        }
    }

    private void printYMDuration(PrintStream ps, TaggedValuePointable tvp) {
        IntegerPointable ip = pp.takeOne(IntegerPointable.class);
        try {
            tvp.getValue(ip);
            abvs.reset();
            castToString.convertYMDuration(ip, dOut);
            printStringAbvs(ps);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(ip);
        }
    }

    @Override
    public void init() throws HyracksDataException {

    }
}
