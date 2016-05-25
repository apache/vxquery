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
package org.apache.vxquery.runtime.functions.index;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IUnnestingEvaluator;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.index.IndexAttributes;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentUnnestingEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentUnnestingEvaluatorFactory;
import org.apache.vxquery.xmlparser.ITreeNodeIdProvider;
import org.apache.vxquery.xmlparser.SAXContentHandler;
import org.apache.vxquery.xmlparser.TreeNodeIdProvider;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

public class CollectionFromIndexUnnestingEvaluatorFactory extends AbstractTaggedValueArgumentUnnestingEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public CollectionFromIndexUnnestingEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IUnnestingEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {

        return new AbstractTaggedValueArgumentUnnestingEvaluator(args) {

            private boolean first;
            private ArrayBackedValueStorage nodeAbvs;

            private int indexplace;
            private int indexlength;
            private String elementpath;
            private String IndexName;

            private UTF8StringPointable stringindexfolder = (UTF8StringPointable) UTF8StringPointable.FACTORY
                    .createPointable();
            private UTF8StringPointable stringelementpath = (UTF8StringPointable) UTF8StringPointable.FACTORY
                    .createPointable();
            private ByteBufferInputStream bbis = new ByteBufferInputStream();
            private DataInputStream di = new DataInputStream(bbis);

            private IndexReader reader;
            private IndexSearcher searcher;
            private Analyzer analyzer;
            private QueryParser parser;
            ScoreDoc[] hits;
            SAXContentHandler handler;
            Query query;
            Document doc;
            List<IndexableField> fields;

            @Override
            public boolean step(IPointable result) throws AlgebricksException {
                /* each step will create a tuple for a single xml file
                 * This is done using the parse function
                 * checkoverflow is used throughout. This is because memory might not be
                 * able to hold all of the results at once, so we return 1 million at
                 * a time and check when we need to get more
                 */
                if (indexplace < indexlength) {
                    int partition = ctxview.getTaskAttemptId().getTaskId().getPartition();
                    ITreeNodeIdProvider nodeIdProvider = new TreeNodeIdProvider((short) partition);
                    handler = new SAXContentHandler(false, nodeIdProvider, true);
                    nodeAbvs.reset();
                    try {
                        doc = searcher.doc(hits[indexplace].doc);
                        fields = doc.getFields();
                        parse(nodeAbvs);
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    indexplace += 1;
                    result.set(nodeAbvs.getByteArray(), nodeAbvs.getStartOffset(), nodeAbvs.getLength());
                    return true;
                }
                return false;
            }

            @Override
            protected void init(TaggedValuePointable[] args) throws SystemException {
                first = true;
                if (first) {
                    nodeAbvs = new ArrayBackedValueStorage();
                    indexplace = 0;
                    TaggedValuePointable tvp1 = args[0];
                    TaggedValuePointable tvp2 = args[1];

                    // TODO add support empty sequence and no argument.
                    if (tvp1.getTag() != ValueTag.XS_STRING_TAG || tvp2.getTag() != ValueTag.XS_STRING_TAG) {
                        throw new SystemException(ErrorCode.FORG0006);
                    }
                    tvp1.getValue(stringindexfolder);
                    tvp2.getValue(stringelementpath);
                    //This whole loop is to get the string arguments, indefolder, elementpath, and match option
                    try {
                        // Get the list of files.
                        bbis.setByteBuffer(ByteBuffer.wrap(
                                Arrays.copyOfRange(stringindexfolder.getByteArray(), stringindexfolder.getStartOffset(),
                                        stringindexfolder.getLength() + stringindexfolder.getStartOffset())),
                                0);
                        IndexName = di.readUTF();
                        bbis.setByteBuffer(ByteBuffer.wrap(
                                Arrays.copyOfRange(stringelementpath.getByteArray(), stringelementpath.getStartOffset(),
                                        stringelementpath.getLength() + stringelementpath.getStartOffset())),
                                0);
                        elementpath = di.readUTF();
                    } catch (IOException e) {
                        throw new SystemException(ErrorCode.SYSE0001, e);
                    }
                    indexplace = 0;
                    first = false;
                    reader = null;
                    //Create the index reader.
                    try {
                        reader = DirectoryReader.open(FSDirectory.open(new File(IndexName)));
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }

                    searcher = new IndexSearcher(reader);
                    analyzer = new StandardAnalyzer(Version.LUCENE_40);

                    parser = new QueryParser(Version.LUCENE_40, "item", analyzer);

                    String queryString = elementpath.replaceAll("/", ".");
                    queryString = "item:" + queryString + "*";

                    int lastslash = elementpath.lastIndexOf("/");
                    elementpath = elementpath.substring(0, lastslash) + ":" + elementpath.substring(lastslash + 1);
                    elementpath = elementpath.replaceAll("/", ".") + ".element";

                    TopDocs results = null;
                    try {
                        query = parser.parse(queryString);
                        System.out.println(query.toString());
                        try {
                            //TODO: Right now it only returns 1000000 results
                            results = searcher.search(query, 1000000);
                        } catch (IOException e) {
                            throw new SystemException(null);
                        }
                    } catch (ParseException e) {
                        throw new SystemException(null);
                    }

                    hits = results.scoreDocs;
                    System.out.println("found: " + results.totalHits);
                    indexplace = 0;
                    indexlength = hits.length;
                }
            }

            public void parse(ArrayBackedValueStorage abvsFileNode) throws IOException {
                try {
                    handler.startDocument();

                    for (int i = 0; i < fields.size(); i++) {
                        if (fields.get(i).stringValue().equals(elementpath)) {
                            buildelement(abvsFileNode, i);
                        }
                    }

                    handler.endDocument();
                    handler.writeDocument(abvsFileNode);
                } catch (SAXException | IOException e) {
                    throw new IOException(e);
                }
            }

            private int buildelement(ArrayBackedValueStorage abvsFileNode, int fieldnum) throws SAXException {
                int whereIFinish = fieldnum;
                IndexableField field = fields.get(fieldnum);
                String contents = field.stringValue();
                String uri = "";

                int firstColon = contents.indexOf(":");
                int lastdot = contents.lastIndexOf(".");
                String type = contents.substring(lastdot + 1);
                String lastbit = contents.substring(firstColon + 1, lastdot);

                if (type.equals("textnode")) {
                    char[] charcontents = lastbit.toCharArray();
                    handler.characters(charcontents, 0, charcontents.length);

                }
                if (type.equals("element")) {
                    Vector<String> names = new Vector<String>();
                    Vector<String> values = new Vector<String>();
                    Vector<String> uris = new Vector<String>();
                    Vector<String> localnames = new Vector<String>();
                    Vector<String> types = new Vector<String>();
                    Vector<String> qnames = new Vector<String>();
                    whereIFinish = findAttributeChildren(whereIFinish, names, values, uris, localnames, types, qnames);
                    Attributes atts = new IndexAttributes(names, values, uris, localnames, types, qnames);
                    try {

                        handler.startElement(uri, lastbit, lastbit, atts);

                        boolean noMoreChildren = false;

                        while (whereIFinish + 1 < fields.size() && !noMoreChildren) {
                            if (isChild(fields.get(whereIFinish + 1), field)) {
                                whereIFinish = buildelement(abvsFileNode, whereIFinish + 1);
                            } else {
                                noMoreChildren = true;
                            }
                        }

                        handler.endElement(uri, lastbit, lastbit);
                    } catch (SAXException e) {
                        e.printStackTrace();
                    }

                }
                return whereIFinish;
            }

            /*This function creates the attribute children for an element node
             * 
             */
            int findAttributeChildren(int fieldnum, Vector<String> n, Vector<String> v, Vector<String> u,
                    Vector<String> l, Vector<String> t, Vector<String> q) {
                int nextindex = fieldnum + 1;
                boolean foundattributes = false;
                if (nextindex < fields.size()) {
                    IndexableField nextguy;

                    while (nextindex < fields.size()) {
                        nextguy = fields.get(nextindex);
                        String contents = nextguy.stringValue();
                        int firstcolon = contents.indexOf(":");
                        int lastdot = contents.lastIndexOf(".");
                        String lastbit = contents.substring(firstcolon + 1, lastdot);

                        if (isDirectChildAttribute(nextguy, fields.get(fieldnum))) {
                            foundattributes = true;
                            n.add(lastbit);
                            IndexableField nextnextguy = fields.get(nextindex + 1);
                            contents = nextnextguy.stringValue();
                            firstcolon = contents.indexOf(":");
                            lastdot = contents.lastIndexOf(".");
                            String nextlastbit = contents.substring(firstcolon + 1, lastdot);
                            v.add(nextlastbit);
                            u.add(lastbit);
                            l.add(lastbit);
                            t.add(lastbit);
                            q.add(lastbit);
                        } else {
                            break;
                        }
                        nextindex += 2;
                    }
                }
                if (foundattributes) {
                    return nextindex - 1;

                } else {
                    return fieldnum;
                }
            }

            boolean isChild(IndexableField child, IndexableField adult) {
                String childid = child.stringValue();
                String adultid = adult.stringValue();

                int lastdotchild = childid.lastIndexOf(".");
                int lastdotadult = adultid.lastIndexOf(".");

                String childpath = childid.substring(0, lastdotchild);
                String adultpath = adultid.substring(0, lastdotadult);
                adultpath = adultpath.replaceFirst(":", ".");

                if (childpath.startsWith(adultpath + ":") || childpath.startsWith(adultpath + ".")) {
                    return true;
                }
                return false;
            }

            boolean isDirectChildAttribute(IndexableField child, IndexableField adult) {
                String childid = child.stringValue();
                String adultid = adult.stringValue();

                String childpath = childid.substring(0, childid.lastIndexOf("."));
                String adultpath = adultid.substring(0, adultid.lastIndexOf("."));
                adultpath = adultpath.replaceFirst(":", ".");
                String[] childpieces = child.stringValue().split("\\.");

                String childtype = childpieces[childpieces.length - 1];

                if (childpath.startsWith(adultpath + ":") && childtype.equals("attribute")) {
                    return true;
                }
                return false;
            }

        };
    }
}