/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.vxquery.index;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hyracks.data.std.api.IPointable;
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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
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
import org.apache.vxquery.datamodel.accessors.nodes.AttributeNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.DocumentNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.ElementNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.NodeTreePointable;
import org.apache.vxquery.datamodel.accessors.nodes.TextOrCommentNodePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.runtime.functions.cast.CastToStringOperation;
import org.apache.vxquery.runtime.functions.index.updateIndex.Constants;
import org.apache.vxquery.serializer.XMLSerializer;

public class IndexDocumentBuilder extends XMLSerializer {
    private final IPointable treePointable;

    private final PointablePool pp;
    private NodeTreePointable ntp;

    private final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
    private final DataOutput dOut = abvs.getDataOutput();
    private final CastToStringOperation castToString = new CastToStringOperation();
    private final Document doc;
    private final List<ComplexItem> results;

    private final byte[] bstart;
    private final int sstart;
    private final int lstart;
    private final IndexWriter writer;
    private final String filePath;

    class ComplexItem {
        public final StringField sf;
        public final String id;

        public ComplexItem(StringField sfin, String idin) {
            sf = sfin;
            id = idin;
        }
    }

    //TODO: Handle Processing Instructions, PrefixedNames, and Namepsace entries
    public IndexDocumentBuilder(IPointable tree, IndexWriter inWriter, String file) {
        this.treePointable = tree;
        writer = inWriter;

        this.filePath = file;

        //convert to tagged value pointable
        TaggedValuePointable tvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        tvp.set(treePointable.getByteArray(), 0, treePointable.getLength());

        //get bytes and info from doc pointer
        bstart = tvp.getByteArray();
        sstart = tvp.getStartOffset();
        lstart = tvp.getLength();

        doc = new Document();

        results = new ArrayList<ComplexItem>();

        pp = PointablePoolFactory.INSTANCE.createPointablePool();
    }

    //This is a wrapper to start indexing using the functions adapted from XMLSerializer
    public void printStart() throws IOException {

        doc.add(new StringField(Constants.FIELD_PATH, filePath, Field.Store.YES));
        print(bstart, sstart, lstart, "0", "");
        for (int i = 1; i < results.size() - 1; i++) {
            //TODO: Since each doc is a file,
            //we can only handle files
            //small enough to fit in memory
            doc.add(results.get(i).sf);
        }
        writer.addDocument(doc);

    }

    //adapted from XMLSerializer. The following functions are used to traverse the TaggedValuePointable
    //and create the index elements, then create the item for the lucene index
    public void print(byte[] b, int s, int l, String deweyId, String epath) throws IOException {
        TaggedValuePointable tvp = pp.takeOne(TaggedValuePointable.class);
        try {
            tvp.set(b, s, l);
            printTaggedValuePointable(tvp, deweyId, epath);
        } finally {
            pp.giveBack(tvp);
        }
    }

    private void printTaggedValuePointable(TaggedValuePointable tvp, String deweyId, String epath) throws IOException {
        byte tag = tvp.getTag();
        String type = "text";
        String[] result = { "", "" };
        switch ((int) tag) {
            case ValueTag.XS_ANY_URI_TAG:
                result = printString(tvp, epath);
                break;

            case ValueTag.XS_BASE64_BINARY_TAG:
                result = printBase64Binary(tvp, epath);
                break;

            case ValueTag.XS_BOOLEAN_TAG:
                result = printBoolean(tvp, epath);
                break;

            case ValueTag.XS_DATE_TAG:
                result = printDate(tvp, epath);
                break;

            case ValueTag.XS_DATETIME_TAG:
                result = printDateTime(tvp, epath);
                break;

            case ValueTag.XS_DAY_TIME_DURATION_TAG:
                result = printDTDuration(tvp, epath);
                break;

            case ValueTag.XS_BYTE_TAG:
                result = printByte(tvp, epath);
                break;

            case ValueTag.XS_DECIMAL_TAG:
                result = printDecimal(tvp, epath);
                break;

            case ValueTag.XS_DOUBLE_TAG:
                result = printDouble(tvp, epath);
                break;

            case ValueTag.XS_DURATION_TAG:
                result = printDuration(tvp, epath);
                break;

            case ValueTag.XS_FLOAT_TAG:
                result = printFloat(tvp, epath);
                break;

            case ValueTag.XS_G_DAY_TAG:
                result = printGDay(tvp, epath);
                break;

            case ValueTag.XS_G_MONTH_TAG:
                result = printGMonth(tvp, epath);
                break;

            case ValueTag.XS_G_MONTH_DAY_TAG:
                result = printGMonthDay(tvp, epath);
                break;

            case ValueTag.XS_G_YEAR_TAG:
                result = printGYear(tvp, epath);
                break;

            case ValueTag.XS_G_YEAR_MONTH_TAG:
                result = printGYearMonth(tvp, epath);
                break;

            case ValueTag.XS_HEX_BINARY_TAG:
                result = printHexBinary(tvp, epath);
                break;

            case ValueTag.XS_INT_TAG:
            case ValueTag.XS_UNSIGNED_SHORT_TAG:
                result = printInt(tvp, epath);
                break;

            case ValueTag.XS_INTEGER_TAG:
            case ValueTag.XS_LONG_TAG:
            case ValueTag.XS_NEGATIVE_INTEGER_TAG:
            case ValueTag.XS_NON_POSITIVE_INTEGER_TAG:
            case ValueTag.XS_NON_NEGATIVE_INTEGER_TAG:
            case ValueTag.XS_POSITIVE_INTEGER_TAG:
            case ValueTag.XS_UNSIGNED_INT_TAG:
            case ValueTag.XS_UNSIGNED_LONG_TAG:
                result = printInteger(tvp, epath);
                break;

            case ValueTag.XS_NOTATION_TAG:
                result = printString(tvp, epath);
                break;

            case ValueTag.XS_QNAME_TAG:
                result = printQName(tvp, epath);
                break;

            case ValueTag.XS_SHORT_TAG:
            case ValueTag.XS_UNSIGNED_BYTE_TAG:
                result = printShort(tvp, epath);
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
                result = printString(tvp, epath);
                break;

            case ValueTag.XS_TIME_TAG:
                result = printTime(tvp, epath);
                break;

            case ValueTag.XS_UNTYPED_ATOMIC_TAG:
                result = printString(tvp, epath);
                break;

            case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                result = printYMDuration(tvp, epath);
                break;

            case ValueTag.ATTRIBUTE_NODE_TAG:
                type = "attribute";
                printAttributeNode(tvp, deweyId, epath);
                break;

            case ValueTag.TEXT_NODE_TAG:
                type = "textnode";
                result = printTextNode(tvp, epath);
                break;

            case ValueTag.COMMENT_NODE_TAG:
                type = "comment";
                result = printCommentNode(tvp, epath);
                break;

            case ValueTag.SEQUENCE_TAG:
                type = "sequence";
                printSequence(tvp, deweyId, epath);
                break;

            case ValueTag.NODE_TREE_TAG:
                type = "tree";
                printNodeTree(tvp, deweyId, epath);
                break;

            case ValueTag.ELEMENT_NODE_TAG:
                type = "element";
                printElementNode(tvp, deweyId, epath);
                break;

            case ValueTag.DOCUMENT_NODE_TAG:
                type = "doc";
                buildIndexItem(deweyId, type, result, epath);
                printDocumentNode(tvp, deweyId, epath);
                break;

            default:
                throw new UnsupportedOperationException("Encountered tag: " + tvp.getTag());
        }
        if ((int) tag != ValueTag.DOCUMENT_NODE_TAG && (int) tag != ValueTag.SEQUENCE_TAG
                && (int) tag != ValueTag.NODE_TREE_TAG && (int) tag != ValueTag.ELEMENT_NODE_TAG
                && (int) tag != ValueTag.ATTRIBUTE_NODE_TAG) {
            buildIndexItem(deweyId, type, result, epath);
        }

    }

    private void buildIndexItem(String deweyId, String type, String[] result, String parentPath) {
        //Create an Index element
        IndexElement test = new IndexElement(deweyId, type, result[1]);

        String path = test.epath();

        path = StringUtils.replace(path, parentPath, "");
        //Parser doesn't like / so paths are saved as name.name....
        String luceneParentPath = parentPath.replaceAll("/", ".");

        if (!type.equals("doc")) {
            path = path.replaceFirst("/", ":");
        } else {
            luceneParentPath = "";
        }
        //Parser doesn't like / so paths are saved as name.name....
        path = path.replaceAll("/", ".");
        //Add this element to the array (they will be added in reverse order.
        String fullItem = luceneParentPath + path + "." + test.type();

        results.add(new ComplexItem(new StringField("item", fullItem, Field.Store.YES), test.id()));
    }

    private String[] printDecimal(TaggedValuePointable tvp, String path) throws IOException {
        String[] result = { "", path };
        XSDecimalPointable dp = pp.takeOne(XSDecimalPointable.class);
        try {
            tvp.getValue(dp);
            abvs.reset();
            castToString.convertDecimal(dp, dOut);
            result = printStringAbvs(path);
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            pp.giveBack(dp);
        }
        return result;
    }

    private void printNodeTree(TaggedValuePointable tvp, String deweyId, String path) throws IOException {
        if (ntp != null) {
            throw new IllegalStateException("Nested NodeTreePointable found");
        }
        ntp = pp.takeOne(NodeTreePointable.class);
        TaggedValuePointable rootTVP = pp.takeOne(TaggedValuePointable.class);
        try {
            tvp.getValue(ntp);
            ntp.getRootNode(rootTVP);
            printTaggedValuePointable(rootTVP, deweyId, path);
        } finally {
            pp.giveBack(rootTVP);
            pp.giveBack(ntp);
            ntp = null;
        }
    }

    private String[] printCommentNode(TaggedValuePointable tvp, String path) {
        String[] result = { "", path };
        TextOrCommentNodePointable tcnp = pp.takeOne(TextOrCommentNodePointable.class);
        UTF8StringPointable utf8sp = pp.takeOne(UTF8StringPointable.class);
        try {
            tvp.getValue(tcnp);
            tcnp.getValue(ntp, utf8sp);

            result = printString(utf8sp, path);

        } finally {
            pp.giveBack(tcnp);
            pp.giveBack(utf8sp);
        }
        return result;
    }

    private String[] printTextNode(TaggedValuePointable tvp, String path) {
        String[] result = { "", path };
        TextOrCommentNodePointable tcnp = pp.takeOne(TextOrCommentNodePointable.class);
        UTF8StringPointable utf8sp = pp.takeOne(UTF8StringPointable.class);
        try {
            tvp.getValue(tcnp);
            tcnp.getValue(ntp, utf8sp);
            result = printString(utf8sp, path);
        } finally {
            pp.giveBack(tcnp);
            pp.giveBack(utf8sp);
        }
        return result;
    }

    private void printAttributeNode(TaggedValuePointable tvp, String deweyId, String path) throws IOException {
        String[] result = { "", path };
        AttributeNodePointable anp = pp.takeOne(AttributeNodePointable.class);
        CodedQNamePointable cqp = pp.takeOne(CodedQNamePointable.class);
        UTF8StringPointable utf8sp = pp.takeOne(UTF8StringPointable.class);
        TaggedValuePointable valueTVP = pp.takeOne(TaggedValuePointable.class);
        try {
            tvp.getValue(anp);
            anp.getName(cqp);
            result = printPrefixedQName(cqp, utf8sp, path);
            buildIndexItem(deweyId, "attribute", result, path);

            anp.getValue(ntp, valueTVP);

            String attributeValueId = deweyId + ".0";
            printTaggedValuePointable(valueTVP, attributeValueId, result[1]);

        } finally {
            pp.giveBack(valueTVP);
            pp.giveBack(utf8sp);
            pp.giveBack(anp);
            pp.giveBack(cqp);
        }
    }

    private void printElementNode(TaggedValuePointable tvp, String deweyId, String path) throws IOException {
        String[] result = { "", path };
        ElementNodePointable enp = pp.takeOne(ElementNodePointable.class);
        CodedQNamePointable cqp = pp.takeOne(CodedQNamePointable.class);
        UTF8StringPointable utf8sp = pp.takeOne(UTF8StringPointable.class);
        SequencePointable seqp = pp.takeOne(SequencePointable.class);
        try {
            tvp.getValue(enp);
            enp.getName(cqp);
            result = printPrefixedQName(cqp, utf8sp, path);
            buildIndexItem(deweyId, "element", result, path);

            enp.getAttributeSequence(ntp, seqp);
            int numattributes = 0;
            if (seqp.getByteArray() != null && seqp.getEntryCount() > 0) {
                printSequence(seqp, deweyId, 0, result[1]);
                numattributes = seqp.getEntryCount();
            }

            enp.getChildrenSequence(ntp, seqp);
            if (seqp.getByteArray() != null) {
                printSequence(seqp, deweyId, numattributes, result[1]);
            }

        } finally {
            pp.giveBack(seqp);
            pp.giveBack(utf8sp);
            pp.giveBack(cqp);
            pp.giveBack(enp);
        }
    }

    private String[] printPrefixedQName(CodedQNamePointable cqp, UTF8StringPointable utf8sp, String path) {
        ntp.getString(cqp.getLocalCode(), utf8sp);
        return printString(utf8sp, path);
    }

    private void printDocumentNode(TaggedValuePointable tvp, String deweyId, String path) throws IOException {
        DocumentNodePointable dnp = pp.takeOne(DocumentNodePointable.class);
        SequencePointable seqp = pp.takeOne(SequencePointable.class);
        try {
            tvp.getValue(dnp);
            dnp.getContent(ntp, seqp);
            printSequence(seqp, deweyId, 0, path);
        } finally {
            pp.giveBack(seqp);
            pp.giveBack(dnp);
        }
    }

    private void printSequence(TaggedValuePointable tvp, String deweyId, String path) throws IOException {
        SequencePointable seqp = pp.takeOne(SequencePointable.class);
        try {
            tvp.getValue(seqp);
            printSequence(seqp, deweyId, 0, path);
        } finally {
            pp.giveBack(seqp);
        }
    }

    private void printSequence(SequencePointable seqp, String deweyId, int addon, String path) throws IOException {
        VoidPointable vp = pp.takeOne(VoidPointable.class);
        try {
            int len = seqp.getEntryCount();
            for (int i = 0; i < len; ++i) {
                int location = i + addon;
                String childID = deweyId + "." + Integer.toString(location);
                seqp.getEntry(i, vp);
                print(vp.getByteArray(), vp.getStartOffset(), vp.getLength(), childID, path);
            }
        } finally {
            pp.giveBack(vp);
        }
    }

    private String[] printBase64Binary(TaggedValuePointable tvp, String path) throws IOException {
        String[] result = { "", path };
        XSBinaryPointable bp = pp.takeOne(XSBinaryPointable.class);
        try {
            tvp.getValue(bp);
            abvs.reset();
            castToString.convertBase64Binary(bp, dOut);
            result = printStringAbvs(path);
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            pp.giveBack(bp);
        }
        return result;
    }

    private String[] printBoolean(TaggedValuePointable tvp, String path) {
        String[] result = { "", path };
        BooleanPointable bp = pp.takeOne(BooleanPointable.class);
        try {
            tvp.getValue(bp);
            result[0] = Boolean.toString(bp.getBoolean());
            result[1] = path + "/" + result[0];
        } finally {
            pp.giveBack(bp);
        }
        return result;
    }

    private String[] printByte(TaggedValuePointable tvp, String path) {
        String[] result = { "", path };
        BytePointable bp = pp.takeOne(BytePointable.class);
        try {
            tvp.getValue(bp);
            result[0] = Byte.toString(bp.byteValue());
            result[1] = path + "/" + result[0];
        } finally {
            pp.giveBack(bp);
        }
        return result;
    }

    private String[] printDouble(TaggedValuePointable tvp, String path) throws IOException {
        String[] result = { "", path };
        DoublePointable dp = pp.takeOne(DoublePointable.class);
        try {
            tvp.getValue(dp);
            abvs.reset();
            castToString.convertDouble(dp, dOut);
            result = printStringAbvs(path);
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            pp.giveBack(dp);
        }
        return result;
    }

    private String[] printDate(TaggedValuePointable tvp, String path) throws IOException {
        String[] result = { "", path };
        XSDatePointable dp = pp.takeOne(XSDatePointable.class);
        try {
            tvp.getValue(dp);
            abvs.reset();
            castToString.convertDate(dp, dOut);
            result = printStringAbvs(path);
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            pp.giveBack(dp);
        }
        return result;
    }

    private String[] printDateTime(TaggedValuePointable tvp, String path) throws IOException {
        String[] result = { "", path };
        XSDateTimePointable dtp = pp.takeOne(XSDateTimePointable.class);
        try {
            tvp.getValue(dtp);
            abvs.reset();
            castToString.convertDatetime(dtp, dOut);
            result = printStringAbvs(path);
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            pp.giveBack(dtp);
        }
        return result;
    }

    private String[] printDTDuration(TaggedValuePointable tvp, String path) throws IOException {
        String[] result = { "", path };
        LongPointable lp = pp.takeOne(LongPointable.class);
        try {
            tvp.getValue(lp);
            abvs.reset();
            castToString.convertDTDuration(lp, dOut);
            result = printStringAbvs(path);
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            pp.giveBack(lp);
        }
        return result;
    }

    private String[] printDuration(TaggedValuePointable tvp, String path) throws IOException {
        String[] result = { "", path };
        XSDurationPointable dp = pp.takeOne(XSDurationPointable.class);
        try {
            tvp.getValue(dp);
            abvs.reset();
            castToString.convertDuration(dp, dOut);
            result = printStringAbvs(path);
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            pp.giveBack(dp);
        }
        return result;
    }

    private String[] printFloat(TaggedValuePointable tvp, String path) throws IOException {
        String[] result = { "", path };
        FloatPointable fp = pp.takeOne(FloatPointable.class);
        try {
            tvp.getValue(fp);
            abvs.reset();
            castToString.convertFloat(fp, dOut);
            result = printStringAbvs(path);
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            pp.giveBack(fp);
        }
        return result;
    }

    private String[] printGDay(TaggedValuePointable tvp, String path) throws IOException {
        String[] result = { "", path };
        XSDatePointable dp = pp.takeOne(XSDatePointable.class);
        try {
            tvp.getValue(dp);
            abvs.reset();
            castToString.convertGDay(dp, dOut);
            result = printStringAbvs(path);
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            pp.giveBack(dp);
        }
        return result;
    }

    private String[] printGMonth(TaggedValuePointable tvp, String path) throws IOException {
        String[] result = { "", path };
        XSDatePointable dp = pp.takeOne(XSDatePointable.class);
        try {
            tvp.getValue(dp);
            abvs.reset();
            castToString.convertGMonth(dp, dOut);
            result = printStringAbvs(path);
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            pp.giveBack(dp);
        }
        return result;
    }

    private String[] printGMonthDay(TaggedValuePointable tvp, String path) throws IOException {
        String[] result = { "", path };
        XSDatePointable dp = pp.takeOne(XSDatePointable.class);
        try {
            tvp.getValue(dp);
            abvs.reset();
            castToString.convertGMonthDay(dp, dOut);
            result = printStringAbvs(path);
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            pp.giveBack(dp);
        }
        return result;
    }

    private String[] printGYear(TaggedValuePointable tvp, String path) throws IOException {
        String[] result = { "", path };
        XSDatePointable dp = pp.takeOne(XSDatePointable.class);
        try {
            tvp.getValue(dp);
            abvs.reset();
            castToString.convertGYear(dp, dOut);
            result = printStringAbvs(path);
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            pp.giveBack(dp);
        }
        return result;
    }

    private String[] printGYearMonth(TaggedValuePointable tvp, String path) throws IOException {
        String[] result = { "", path };
        XSDatePointable dp = pp.takeOne(XSDatePointable.class);
        try {
            tvp.getValue(dp);
            abvs.reset();
            castToString.convertGYearMonth(dp, dOut);
            result = printStringAbvs(path);
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            pp.giveBack(dp);
        }
        return result;
    }

    private String[] printHexBinary(TaggedValuePointable tvp, String path) throws IOException {
        String[] result = { "", path };
        XSBinaryPointable bp = pp.takeOne(XSBinaryPointable.class);
        try {
            tvp.getValue(bp);
            abvs.reset();
            castToString.convertHexBinary(bp, dOut);
            result = printStringAbvs(path);
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            pp.giveBack(bp);
        }
        return result;
    }

    private String[] printInt(TaggedValuePointable tvp, String path) {
        String[] result = { "", path };
        IntegerPointable ip = pp.takeOne(IntegerPointable.class);
        try {
            tvp.getValue(ip);
            result[0] = Integer.toString(ip.intValue());
            result[1] = path + "/" + result[0];
        } finally {
            pp.giveBack(ip);
        }
        return result;
    }

    private String[] printInteger(TaggedValuePointable tvp, String path) {
        String[] result = { "", path };
        LongPointable lp = pp.takeOne(LongPointable.class);
        try {
            tvp.getValue(lp);
            result[0] = Long.toString(lp.longValue());
            result[1] = path + "/" + result[0];
        } finally {
            pp.giveBack(lp);
        }
        return result;
    }

    private String[] printShort(TaggedValuePointable tvp, String path) {
        ShortPointable sp = pp.takeOne(ShortPointable.class);
        String[] result = { "", path };
        try {
            tvp.getValue(sp);
            result[0] = Short.toString(sp.shortValue());
            result[1] = path + "/" + result[0];
        } finally {
            pp.giveBack(sp);
        }
        return result;
    }

    private String[] printQName(TaggedValuePointable tvp, String path) throws IOException {
        XSQNamePointable dp = pp.takeOne(XSQNamePointable.class);
        String[] result = { "", path };
        try {
            tvp.getValue(dp);
            abvs.reset();
            castToString.convertQName(dp, dOut);
            result = printStringAbvs(path);
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            pp.giveBack(dp);
        }
        return result;
    }

    private String[] printStringAbvs(String path) {
        UTF8StringPointable utf8sp = pp.takeOne(UTF8StringPointable.class);
        String[] result = { "", path };
        try {
            utf8sp.set(abvs.getByteArray(), abvs.getStartOffset() + 1, abvs.getLength() - 1);
            result = printString(utf8sp, path);
        } finally {
            pp.giveBack(utf8sp);
        }
        return result;
    }

    private String[] printString(TaggedValuePointable tvp, String path) {
        UTF8StringPointable utf8sp = pp.takeOne(UTF8StringPointable.class);
        String[] result = { "", path };
        try {
            tvp.getValue(utf8sp);
            result = printString(utf8sp, path);
        } finally {
            pp.giveBack(utf8sp);
        }
        return result;
    }

    private String[] printString(UTF8StringPointable utf8sp, String path) {
        int utfLen = utf8sp.getUTF8Length();
        int offset = utf8sp.getMetaDataLength();
        String[] result = { "", path };
        while (utfLen > 0) {
            char c = utf8sp.charAt(offset);
            switch (c) {
                case '<':
                    result[0] += "&lt;";
                    break;

                case '>':
                    result[0] += "&gt;";
                    break;

                case '&':
                    result[0] += "&amp;";
                    break;

                case '"':
                    result[0] += "&quot;";
                    break;

                case '\'':
                    result[0] += "&apos;";
                    break;

                default:
                    result[0] += Character.toString(c);
                    break;
            }
            int cLen = utf8sp.charSize(offset);
            offset += cLen;
            utfLen -= cLen;

        }
        result[1] = path + "/" + result[0];
        return result;
    }

    private String[] printTime(TaggedValuePointable tvp, String path) throws IOException {
        XSTimePointable tp = pp.takeOne(XSTimePointable.class);
        String[] result = { "", path };
        try {
            tvp.getValue(tp);
            abvs.reset();
            castToString.convertTime(tp, dOut);
            result = printStringAbvs(path);
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            pp.giveBack(tp);
        }
        return result;
    }

    private String[] printYMDuration(TaggedValuePointable tvp, String path) throws IOException {
        IntegerPointable ip = pp.takeOne(IntegerPointable.class);
        String[] result = { "", path };
        try {
            tvp.getValue(ip);
            abvs.reset();
            castToString.convertYMDuration(ip, dOut);
            result = printStringAbvs(path);
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            pp.giveBack(ip);
        }
        return result;
    }

}