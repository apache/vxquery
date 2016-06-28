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
package org.apache.vxquery.runtime.functions.index;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
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

            private ArrayBackedValueStorage nodeAbvs = new ArrayBackedValueStorage();

            private int indexPlace;
            private int indexLength;
            private String elementPath;
            private String indexName;

            private UTF8StringPointable stringIndexFolder = (UTF8StringPointable) UTF8StringPointable.FACTORY
                    .createPointable();
            private UTF8StringPointable stringElementPath = (UTF8StringPointable) UTF8StringPointable.FACTORY
                    .createPointable();
            private ByteBufferInputStream bbis = new ByteBufferInputStream();
            private DataInputStream di = new DataInputStream(bbis);

            private IndexReader reader;
            private IndexSearcher searcher;
            private Analyzer analyzer;
            private QueryParser parser;
            private ScoreDoc[] hits;
            private SAXContentHandler handler;
            private Query query;
            private Document doc;
            private List<IndexableField> fields;

            @Override
            public boolean step(IPointable result) throws AlgebricksException {
                /* each step will create a tuple for a single xml file
                 * This is done using the parse function
                 * checkoverflow is used throughout. This is because memory might not be
                 * able to hold all of the results at once, so we return 1 million at
                 * a time and check when we need to get more
                 */
                if (indexPlace < indexLength) {
                    nodeAbvs.reset();
                    try {
                        //TODO: now we get back the entire document
                        doc = searcher.doc(hits[indexPlace].doc);
                        fields = doc.getFields();
                        parse(nodeAbvs);
                    } catch (IOException e) {
                        throw new AlgebricksException(e);
                    }
                    indexPlace += 1;
                    result.set(nodeAbvs.getByteArray(), nodeAbvs.getStartOffset(), nodeAbvs.getLength());
                    return true;
                }
                return false;
            }

            @Override
            protected void init(TaggedValuePointable[] args) throws SystemException {

                int partition = ctxview.getTaskAttemptId().getTaskId().getPartition();
                ITreeNodeIdProvider nodeIdProvider = new TreeNodeIdProvider((short) partition);
                handler = new SAXContentHandler(false, nodeIdProvider, true);

                nodeAbvs.reset();
                indexPlace = 0;
                TaggedValuePointable tvp1 = args[0];
                TaggedValuePointable tvp2 = args[1];

                if (tvp1.getTag() != ValueTag.XS_STRING_TAG || tvp2.getTag() != ValueTag.XS_STRING_TAG) {
                    throw new SystemException(ErrorCode.FORG0006);
                }
                tvp1.getValue(stringIndexFolder);
                tvp2.getValue(stringElementPath);
                //This whole loop is to get the string arguments, indexFolder, elementPath, and match option
                try {
                    // Get the list of files.
                    bbis.setByteBuffer(ByteBuffer.wrap(
                            Arrays.copyOfRange(stringIndexFolder.getByteArray(), stringIndexFolder.getStartOffset(),
                                    stringIndexFolder.getLength() + stringIndexFolder.getStartOffset())),
                            0);
                    indexName = di.readUTF();
                    bbis.setByteBuffer(ByteBuffer.wrap(
                            Arrays.copyOfRange(stringElementPath.getByteArray(), stringElementPath.getStartOffset(),
                                    stringElementPath.getLength() + stringElementPath.getStartOffset())),
                            0);
                    elementPath = di.readUTF();

                    indexPlace = 0;

                    //Create the index reader.
                    reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexName)));
                } catch (IOException e) {
                    throw new SystemException(ErrorCode.SYSE0001, e);
                }

                searcher = new IndexSearcher(reader);
                analyzer = new CaseSensitiveAnalyzer();

                parser = new CaseSensitiveQueryParser("item", analyzer);

                String queryString = elementPath.replaceAll("/", ".");
                queryString = "item:" + queryString + "*";

                int lastslash = elementPath.lastIndexOf("/");
                elementPath = elementPath.substring(0, lastslash) + ":" + elementPath.substring(lastslash + 1);
                elementPath = elementPath.replaceAll("/", ".") + ".element";

                TopDocs results = null;
                try {
                    query = parser.parse(queryString);

                    //TODO: Right now it only returns 1000000 results
                    results = searcher.search(query, 1000000);

                } catch (Exception e) {
                    throw new SystemException(null);
                }

                hits = results.scoreDocs;
                System.out.println("found: " + results.totalHits);
                indexPlace = 0;
                indexLength = hits.length;

            }

            public void parse(ArrayBackedValueStorage abvsFileNode) throws IOException {
                try {
                    handler.startDocument();

                    for (int i = 0; i < fields.size(); i++) {
                        String fieldValue = fields.get(i).stringValue();
                        if (fieldValue.equals(elementPath)) {
                            buildElement(abvsFileNode, i);
                        }
                    }

                    handler.endDocument();
                    handler.writeDocument(abvsFileNode);
                } catch (Exception e) {
                    throw new IOException(e);
                }
            }

            private int buildElement(ArrayBackedValueStorage abvsFileNode, int fieldNum) throws SAXException {
                int whereIFinish = fieldNum;
                IndexableField field = fields.get(fieldNum);
                String contents = field.stringValue();
                String uri = "";

                int firstColon = contents.indexOf(':');
                int lastDot = contents.lastIndexOf('.');
                String type = contents.substring(lastDot + 1);
                String lastBit = contents.substring(firstColon + 1, lastDot);

                if (type.equals("textnode")) {
                    char[] charContents = lastBit.toCharArray();
                    handler.characters(charContents, 0, charContents.length);

                }
                if (type.equals("element")) {
                    List<String> names = new ArrayList<String>();
                    List<String> values = new ArrayList<String>();
                    List<String> uris = new ArrayList<String>();
                    List<String> localNames = new ArrayList<String>();
                    List<String> types = new ArrayList<String>();
                    List<String> qNames = new ArrayList<String>();
                    whereIFinish = findAttributeChildren(whereIFinish, names, values, uris, localNames, types, qNames);
                    Attributes atts = new IndexAttributes(names, values, uris, localNames, types, qNames);

                    handler.startElement(uri, lastBit, lastBit, atts);

                    boolean noMoreChildren = false;

                    while (whereIFinish + 1 < fields.size() && !noMoreChildren) {
                        if (isChild(fields.get(whereIFinish + 1), field)) {
                            whereIFinish = buildElement(abvsFileNode, whereIFinish + 1);
                        } else {
                            noMoreChildren = true;
                        }
                    }

                    handler.endElement(uri, lastBit, lastBit);

                }
                return whereIFinish;
            }

            /*This function creates the attribute children for an element node
             * 
             */
            int findAttributeChildren(int fieldnum, List<String> n, List<String> v, List<String> u, List<String> l,
                    List<String> t, List<String> q) {
                int nextindex = fieldnum + 1;
                boolean foundattributes = false;
                if (nextindex < fields.size()) {
                    IndexableField nextguy;

                    while (nextindex < fields.size()) {
                        nextguy = fields.get(nextindex);
                        String contents = nextguy.stringValue();
                        int firstcolon = contents.indexOf(':');
                        int lastdot = contents.lastIndexOf('.');
                        String lastbit = contents.substring(firstcolon + 1, lastdot);

                        if (isDirectChildAttribute(nextguy, fields.get(fieldnum))) {
                            foundattributes = true;
                            n.add(lastbit);
                            IndexableField nextnextguy = fields.get(nextindex + 1);
                            contents = nextnextguy.stringValue();
                            firstcolon = contents.indexOf(':');
                            lastdot = contents.lastIndexOf('.');
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
                String childId = child.stringValue();
                String adultId = adult.stringValue();

                int lastDotChild = childId.lastIndexOf('.');
                int lastDotAdult = adultId.lastIndexOf('.');

                String childPath = childId.substring(0, lastDotChild);
                String adultPath = adultId.substring(0, lastDotAdult);
                adultPath = adultPath.replaceFirst(":", ".");

                return (childPath.startsWith(adultPath + ":") || childPath.startsWith(adultPath + "."));
            }

            boolean isDirectChildAttribute(IndexableField child, IndexableField adult) {
                String childId = child.stringValue();
                String adultId = adult.stringValue();

                String childPath = childId.substring(0, childId.lastIndexOf('.'));
                String adultPath = adultId.substring(0, adultId.lastIndexOf('.'));
                adultPath = adultPath.replaceFirst(":", ".");
                String[] childSegments = child.stringValue().split("\\.");

                String childType = childSegments[childSegments.length - 1];

                return (childPath.startsWith(adultPath + ":") && childType.equals("attribute"));
            }

        };
    }
}