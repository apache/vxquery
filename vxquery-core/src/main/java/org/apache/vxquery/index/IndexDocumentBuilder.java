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
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.cast.CastToStringOperation;
import org.apache.vxquery.serializer.XMLSerializer;

public class IndexDocumentBuilder extends XMLSerializer {
    private IPointable treepointable;

    private PointablePool pp;
    private NodeTreePointable ntp;

    private ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
    private DataOutput dOut = abvs.getDataOutput();
    private CastToStringOperation castToString = new CastToStringOperation();
    private Document doc;
    private List<ComplexItem> results;

    private byte[] bstart;
    private int sstart;
    private int lstart;

    String filePath;
    IndexWriter writer;

    class ComplexItem {
        public StringField sf;
        public String id;

        public ComplexItem(StringField sfin, String idin) {
            sf = sfin;
            id = idin;
        }
    }

    //TODO: Handle Processing Instructions, PrefixedNames, and Namepsace entries
    public IndexDocumentBuilder(IPointable tree, IndexWriter inWriter, String inFilePath) {
        this.treepointable = tree;
        writer = inWriter;
        filePath = inFilePath;

        //convert to tagged value pointable
        TaggedValuePointable tvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        tvp.set(treepointable.getByteArray(), 0, treepointable.getLength());

        //get bytes and info from doc pointer
        bstart = tvp.getByteArray();
        sstart = tvp.getStartOffset();
        lstart = tvp.getLength();

        doc = new Document();

        results = new ArrayList<ComplexItem>();

        pp = PointablePoolFactory.INSTANCE.createPointablePool();
    }

    //This is a wrapper to start indexing using the functions adapted from XMLSerializer
    public void printstart() throws IOException {

        print(bstart, sstart, lstart, "0", "");
        for (int i = 1; i < results.size() - 1; i++) {
            //Show the results for debugging
            //System.out.println(results.get(i).sf + " " + results.get(i).id);
            doc.add(results.get(i).sf);
        }
        writer.addDocument(doc);

    }

    //adapted from XMLSerializer. The following functions are used to traverse the TaggedValuePointable
    //and create the index elements, then create the item for the lucene index
    public void print(byte[] b, int s, int l, String myid, String epath) {
        TaggedValuePointable tvp = pp.takeOne(TaggedValuePointable.class);
        try {
            tvp.set(b, s, l);
            printTaggedValuePointable(tvp, myid, epath);
        } finally {
            pp.giveBack(tvp);
        }
    }

    private void printTaggedValuePointable(TaggedValuePointable tvp, String myid, String epath) {
        byte tag = tvp.getTag();
        String type = "text";
        String[] result = { "", "" };
        switch ((int) tag) {
            case ValueTag.XS_ANY_URI_TAG:
                result = printString(tvp, myid, epath);
                break;

            case ValueTag.XS_BASE64_BINARY_TAG:
                result = printBase64Binary(tvp, myid, epath);
                break;

            case ValueTag.XS_BOOLEAN_TAG:
                result = printBoolean(tvp, myid, epath);
                break;

            case ValueTag.XS_DATE_TAG:
                result = printDate(tvp, myid, epath);
                break;

            case ValueTag.XS_DATETIME_TAG:
                result = printDateTime(tvp, myid, epath);
                break;

            case ValueTag.XS_DAY_TIME_DURATION_TAG:
                result = printDTDuration(tvp, myid, epath);
                break;

            case ValueTag.XS_BYTE_TAG:
                result = printByte(tvp, myid, epath);
                break;

            case ValueTag.XS_DECIMAL_TAG:
                result = printDecimal(tvp, myid, epath);
                break;

            case ValueTag.XS_DOUBLE_TAG:
                result = printDouble(tvp, myid, epath);
                break;

            case ValueTag.XS_DURATION_TAG:
                result = printDuration(tvp, myid, epath);
                break;

            case ValueTag.XS_FLOAT_TAG:
                result = printFloat(tvp, myid, epath);
                break;

            case ValueTag.XS_G_DAY_TAG:
                result = printGDay(tvp, myid, epath);
                break;

            case ValueTag.XS_G_MONTH_TAG:
                result = printGMonth(tvp, myid, epath);
                break;

            case ValueTag.XS_G_MONTH_DAY_TAG:
                result = printGMonthDay(tvp, myid, epath);
                break;

            case ValueTag.XS_G_YEAR_TAG:
                result = printGYear(tvp, myid, epath);
                break;

            case ValueTag.XS_G_YEAR_MONTH_TAG:
                result = printGYearMonth(tvp, myid, epath);
                break;

            case ValueTag.XS_HEX_BINARY_TAG:
                result = printHexBinary(tvp, myid, epath);
                break;

            case ValueTag.XS_INT_TAG:
            case ValueTag.XS_UNSIGNED_SHORT_TAG:
                result = printInt(tvp, myid, epath);
                break;

            case ValueTag.XS_INTEGER_TAG:
            case ValueTag.XS_LONG_TAG:
            case ValueTag.XS_NEGATIVE_INTEGER_TAG:
            case ValueTag.XS_NON_POSITIVE_INTEGER_TAG:
            case ValueTag.XS_NON_NEGATIVE_INTEGER_TAG:
            case ValueTag.XS_POSITIVE_INTEGER_TAG:
            case ValueTag.XS_UNSIGNED_INT_TAG:
            case ValueTag.XS_UNSIGNED_LONG_TAG:
                result = printInteger(tvp, myid, epath);
                break;

            case ValueTag.XS_NOTATION_TAG:
                result = printString(tvp, myid, epath);
                break;

            case ValueTag.XS_QNAME_TAG:
                result = printQName(tvp, myid, epath);
                break;

            case ValueTag.XS_SHORT_TAG:
            case ValueTag.XS_UNSIGNED_BYTE_TAG:
                result = printShort(tvp, myid, epath);
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
                result = printString(tvp, myid, epath);
                break;

            case ValueTag.XS_TIME_TAG:
                result = printTime(tvp, myid, epath);
                break;

            case ValueTag.XS_UNTYPED_ATOMIC_TAG:
                result = printString(tvp, myid, epath);
                break;

            case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                result = printYMDuration(tvp, myid, epath);
                break;

            case ValueTag.ATTRIBUTE_NODE_TAG:
                type = "attribute";
                printAttributeNode(tvp, myid, epath);
                break;

            case ValueTag.TEXT_NODE_TAG:
                type = "textnode";
                result = printTextNode(tvp, myid, epath);
                break;

            case ValueTag.COMMENT_NODE_TAG:
                type = "comment";
                result = printCommentNode(tvp, myid, epath);
                break;

            case ValueTag.SEQUENCE_TAG:
                type = "sequence";
                printSequence(tvp, myid, epath);
                break;

            case ValueTag.NODE_TREE_TAG:
                type = "tree";
                printNodeTree(tvp, myid, epath);
                break;

            case ValueTag.ELEMENT_NODE_TAG:
                type = "element";
                printElementNode(tvp, myid, epath);
                break;

            case ValueTag.DOCUMENT_NODE_TAG:
                type = "doc";
                buildIndexItem(myid, type, result, epath);
                printDocumentNode(tvp, myid, epath);
                break;

            default:
                throw new UnsupportedOperationException("Encountered tag: " + tvp.getTag());
        }
        if ((int) tag != ValueTag.DOCUMENT_NODE_TAG && (int) tag != ValueTag.SEQUENCE_TAG
                && (int) tag != ValueTag.NODE_TREE_TAG && (int) tag != ValueTag.ELEMENT_NODE_TAG
                && (int) tag != ValueTag.ATTRIBUTE_NODE_TAG) {
            buildIndexItem(myid, type, result, epath);
        }

    }

    private void buildIndexItem(String myid, String type, String[] result, String parentpath) {
        //Create an Indexelement
        IndexElement test = new IndexElement(myid, type, result[1]);

        String ezpath = test.epath();

        ezpath = StringUtils.replace(ezpath, parentpath, "");
        //Parser doesn't like / so paths are saved as name.name....
        parentpath = parentpath.replaceAll("/", ".");

        if (!type.equals("doc")) {
            ezpath = ezpath.replaceFirst("/", ":");

        } else {
            parentpath = "";
        }
        //Parser doesn't like / so paths are saved as name.name....
        ezpath = ezpath.replaceAll("/", ".");
        //Add this element to the array (they will be added in reverse order.
        String fullitem = parentpath + ezpath + "." + test.type();

        results.add(new ComplexItem(new StringField("item", fullitem, Field.Store.YES), test.id()));
    }

    private String[] printDecimal(TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        XSDecimalPointable dp = pp.takeOne(XSDecimalPointable.class);
        try {
            tvp.getValue(dp);
            abvs.reset();
            castToString.convertDecimal(dp, dOut);
            result = printStringAbvs(myid, epath);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(dp);
        }
        return result;
    }

    private void printNodeTree(TaggedValuePointable tvp, String myid, String epath) {
        if (ntp != null) {
            throw new IllegalStateException("Nested NodeTreePointable found");
        }
        ntp = pp.takeOne(NodeTreePointable.class);
        TaggedValuePointable rootTVP = pp.takeOne(TaggedValuePointable.class);
        try {
            tvp.getValue(ntp);
            ntp.getRootNode(rootTVP);
            printTaggedValuePointable(rootTVP, myid, epath);
        } finally {
            pp.giveBack(rootTVP);
            pp.giveBack(ntp);
            ntp = null;
        }
    }

    private String[] printCommentNode(TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        TextOrCommentNodePointable tcnp = pp.takeOne(TextOrCommentNodePointable.class);
        UTF8StringPointable utf8sp = pp.takeOne(UTF8StringPointable.class);
        try {
            tvp.getValue(tcnp);
            tcnp.getValue(ntp, utf8sp);

            result = printString(utf8sp, myid, epath);

        } finally {
            pp.giveBack(tcnp);
            pp.giveBack(utf8sp);
        }
        return result;
    }

    private String[] printTextNode(TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        TextOrCommentNodePointable tcnp = pp.takeOne(TextOrCommentNodePointable.class);
        UTF8StringPointable utf8sp = pp.takeOne(UTF8StringPointable.class);
        try {
            tvp.getValue(tcnp);
            tcnp.getValue(ntp, utf8sp);
            result = printString(utf8sp, myid, epath);
        } finally {
            pp.giveBack(tcnp);
            pp.giveBack(utf8sp);
        }
        return result;
    }

    private void printAttributeNode(TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        AttributeNodePointable anp = pp.takeOne(AttributeNodePointable.class);
        CodedQNamePointable cqp = pp.takeOne(CodedQNamePointable.class);
        UTF8StringPointable utf8sp = pp.takeOne(UTF8StringPointable.class);
        TaggedValuePointable valueTVP = pp.takeOne(TaggedValuePointable.class);
        try {
            tvp.getValue(anp);
            anp.getName(cqp);
            result = printPrefixedQName(cqp, utf8sp, myid, epath);
            buildIndexItem(myid, "attribute", result, epath);

            anp.getValue(ntp, valueTVP);

            String attributevalueid = myid + ".0";
            printTaggedValuePointable(valueTVP, attributevalueid, result[1]);

        } finally {
            pp.giveBack(valueTVP);
            pp.giveBack(utf8sp);
            pp.giveBack(anp);
            pp.giveBack(cqp);
        }
    }

    private void printElementNode(TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        ElementNodePointable enp = pp.takeOne(ElementNodePointable.class);
        CodedQNamePointable cqp = pp.takeOne(CodedQNamePointable.class);
        UTF8StringPointable utf8sp = pp.takeOne(UTF8StringPointable.class);
        SequencePointable seqp = pp.takeOne(SequencePointable.class);
        try {
            tvp.getValue(enp);
            enp.getName(cqp);
            result = printPrefixedQName(cqp, utf8sp, myid, epath);
            buildIndexItem(myid, "element", result, epath);

            enp.getAttributeSequence(ntp, seqp);
            int numattributes = 0;
            if (seqp.getByteArray() != null && seqp.getEntryCount() > 0) {
                printSequence(seqp, myid, 0, result[1]);
                numattributes = seqp.getEntryCount();
            }

            enp.getChildrenSequence(ntp, seqp);
            if (seqp.getByteArray() != null) {
                printSequence(seqp, myid, numattributes, result[1]);
            }

        } finally {
            pp.giveBack(seqp);
            pp.giveBack(utf8sp);
            pp.giveBack(cqp);
            pp.giveBack(enp);
        }
    }

    private String[] printPrefixedQName(CodedQNamePointable cqp, UTF8StringPointable utf8sp, String myid,
            String epath) {
        ntp.getString(cqp.getLocalCode(), utf8sp);
        return printString(utf8sp, myid, epath);
    }

    private void printDocumentNode(TaggedValuePointable tvp, String myid, String epath) {
        DocumentNodePointable dnp = pp.takeOne(DocumentNodePointable.class);
        SequencePointable seqp = pp.takeOne(SequencePointable.class);
        try {
            tvp.getValue(dnp);
            dnp.getContent(ntp, seqp);
            printSequence(seqp, myid, 0, epath);
        } finally {
            pp.giveBack(seqp);
            pp.giveBack(dnp);
        }
    }

    private void printSequence(TaggedValuePointable tvp, String myid, String epath) {
        SequencePointable seqp = pp.takeOne(SequencePointable.class);
        try {
            tvp.getValue(seqp);
            printSequence(seqp, myid, 0, epath);
        } finally {
            pp.giveBack(seqp);
        }
    }

    private void printSequence(SequencePointable seqp, String myid, int addon, String epath) {
        VoidPointable vp = pp.takeOne(VoidPointable.class);
        try {
            int len = seqp.getEntryCount();
            for (int i = 0; i < len; ++i) {
                int location = i + addon;
                String childid = myid + "." + Integer.toString(location);
                seqp.getEntry(i, vp);
                print(vp.getByteArray(), vp.getStartOffset(), vp.getLength(), childid, epath);
            }
        } finally {
            pp.giveBack(vp);
        }
    }

    private String[] printBase64Binary(TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        XSBinaryPointable bp = pp.takeOne(XSBinaryPointable.class);
        try {
            tvp.getValue(bp);
            abvs.reset();
            castToString.convertBase64Binary(bp, dOut);
            result = printStringAbvs(myid, epath);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(bp);
        }
        return result;
    }

    private String[] printBoolean(TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        BooleanPointable bp = pp.takeOne(BooleanPointable.class);
        try {
            tvp.getValue(bp);
            result[0] = Boolean.toString(bp.getBoolean());
            result[1] = epath + "/" + result[0];
        } finally {
            pp.giveBack(bp);
        }
        return result;
    }

    private String[] printByte(TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        BytePointable bp = pp.takeOne(BytePointable.class);
        try {
            tvp.getValue(bp);
            result[0] = Byte.toString(bp.byteValue());
            result[1] = epath + "/" + result[0];
        } finally {
            pp.giveBack(bp);
        }
        return result;
    }

    private String[] printDouble(TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        DoublePointable dp = pp.takeOne(DoublePointable.class);
        try {
            tvp.getValue(dp);
            abvs.reset();
            castToString.convertDouble(dp, dOut);
            result = printStringAbvs(myid, epath);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(dp);
        }
        return result;
    }

    private String[] printDate(TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        XSDatePointable dp = pp.takeOne(XSDatePointable.class);
        try {
            tvp.getValue(dp);
            abvs.reset();
            castToString.convertDate(dp, dOut);
            result = printStringAbvs(myid, epath);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(dp);
        }
        return result;
    }

    private String[] printDateTime(TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        XSDateTimePointable dtp = pp.takeOne(XSDateTimePointable.class);
        try {
            tvp.getValue(dtp);
            abvs.reset();
            castToString.convertDatetime(dtp, dOut);
            result = printStringAbvs(myid, epath);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(dtp);
        }
        return result;
    }

    private String[] printDTDuration(TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        LongPointable lp = pp.takeOne(LongPointable.class);
        try {
            tvp.getValue(lp);
            abvs.reset();
            castToString.convertDTDuration(lp, dOut);
            result = printStringAbvs(myid, epath);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(lp);
        }
        return result;
    }

    private String[] printDuration(TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        XSDurationPointable dp = pp.takeOne(XSDurationPointable.class);
        try {
            tvp.getValue(dp);
            abvs.reset();
            castToString.convertDuration(dp, dOut);
            result = printStringAbvs(myid, epath);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(dp);
        }
        return result;
    }

    private String[] printFloat(TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        FloatPointable fp = pp.takeOne(FloatPointable.class);
        try {
            tvp.getValue(fp);
            abvs.reset();
            castToString.convertFloat(fp, dOut);
            result = printStringAbvs(myid, epath);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(fp);
        }
        return result;
    }

    private String[] printGDay(TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        XSDatePointable dp = pp.takeOne(XSDatePointable.class);
        try {
            tvp.getValue(dp);
            abvs.reset();
            castToString.convertGDay(dp, dOut);
            result = printStringAbvs(myid, epath);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(dp);
        }
        return result;
    }

    private String[] printGMonth(TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        XSDatePointable dp = pp.takeOne(XSDatePointable.class);
        try {
            tvp.getValue(dp);
            abvs.reset();
            castToString.convertGMonth(dp, dOut);
            result = printStringAbvs(myid, epath);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(dp);
        }
        return result;
    }

    private String[] printGMonthDay(TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        XSDatePointable dp = pp.takeOne(XSDatePointable.class);
        try {
            tvp.getValue(dp);
            abvs.reset();
            castToString.convertGMonthDay(dp, dOut);
            result = printStringAbvs(myid, epath);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(dp);
        }
        return result;
    }

    private String[] printGYear(TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        XSDatePointable dp = pp.takeOne(XSDatePointable.class);
        try {
            tvp.getValue(dp);
            abvs.reset();
            castToString.convertGYear(dp, dOut);
            result = printStringAbvs(myid, epath);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(dp);
        }
        return result;
    }

    private String[] printGYearMonth(TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        XSDatePointable dp = pp.takeOne(XSDatePointable.class);
        try {
            tvp.getValue(dp);
            abvs.reset();
            castToString.convertGYearMonth(dp, dOut);
            result = printStringAbvs(myid, epath);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(dp);
        }
        return result;
    }

    private String[] printHexBinary(TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        XSBinaryPointable bp = pp.takeOne(XSBinaryPointable.class);
        try {
            tvp.getValue(bp);
            abvs.reset();
            castToString.convertHexBinary(bp, dOut);
            result = printStringAbvs(myid, epath);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(bp);
        }
        return result;
    }

    private String[] printInt(TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        IntegerPointable ip = pp.takeOne(IntegerPointable.class);
        try {
            tvp.getValue(ip);
            result[0] = Integer.toString(ip.intValue());
            result[1] = epath + "/" + result[0];
        } finally {
            pp.giveBack(ip);
        }
        return result;
    }

    private String[] printInteger(TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        LongPointable lp = pp.takeOne(LongPointable.class);
        try {
            tvp.getValue(lp);
            result[0] = Long.toString(lp.longValue());
            result[1] = epath + "/" + result[0];
        } finally {
            pp.giveBack(lp);
        }
        return result;
    }

    private String[] printShort(TaggedValuePointable tvp, String myid, String epath) {
        ShortPointable sp = pp.takeOne(ShortPointable.class);
        String[] result = { "", epath };
        try {
            tvp.getValue(sp);
            result[0] = Short.toString(sp.shortValue());
            result[1] = epath + "/" + result[0];
        } finally {
            pp.giveBack(sp);
        }
        return result;
    }

    private String[] printQName(TaggedValuePointable tvp, String myid, String epath) {
        XSQNamePointable dp = pp.takeOne(XSQNamePointable.class);
        String[] result = { "", epath };
        try {
            tvp.getValue(dp);
            abvs.reset();
            castToString.convertQName(dp, dOut);
            result = printStringAbvs(myid, epath);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(dp);
        }
        return result;
    }

    private String[] printStringAbvs(String myid, String epath) {
        UTF8StringPointable utf8sp = pp.takeOne(UTF8StringPointable.class);
        String[] result = { "", epath };
        try {
            utf8sp.set(abvs.getByteArray(), abvs.getStartOffset() + 1, abvs.getLength() - 1);
            result = printString(utf8sp, myid, epath);
        } finally {
            pp.giveBack(utf8sp);
        }
        return result;
    }

    private String[] printString(TaggedValuePointable tvp, String myid, String epath) {
        UTF8StringPointable utf8sp = pp.takeOne(UTF8StringPointable.class);
        String[] result = { "", epath };
        try {
            tvp.getValue(utf8sp);
            result = printString(utf8sp, myid, epath);
        } finally {
            pp.giveBack(utf8sp);
        }
        return result;
    }

    private String[] printString(UTF8StringPointable utf8sp, String myid, String epath) {
        int utfLen = utf8sp.getUTFLength();
        int offset = 2;
        String[] result = { "", epath };
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
                    result[0] += c;
                    break;
            }
            int cLen = UTF8StringPointable.getModifiedUTF8Len(c);
            offset += cLen;
            utfLen -= cLen;

        }
        result[1] = epath + "/" + result[0];
        return result;
    }

    private String[] printTime(TaggedValuePointable tvp, String myid, String epath) {
        XSTimePointable tp = pp.takeOne(XSTimePointable.class);
        String[] result = { "", epath };
        try {
            tvp.getValue(tp);
            abvs.reset();
            castToString.convertTime(tp, dOut);
            result = printStringAbvs(myid, epath);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(tp);
        }
        return result;
    }

    private String[] printYMDuration(TaggedValuePointable tvp, String myid, String epath) {
        IntegerPointable ip = pp.takeOne(IntegerPointable.class);
        String[] result = { "", epath };
        try {
            tvp.getValue(ip);
            abvs.reset();
            castToString.convertYMDuration(ip, dOut);
            result = printStringAbvs(myid, epath);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(ip);
        }
        return result;
    }

}