package org.apache.vxquery.runtime.functions.node;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.builders.sequence.SequenceBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.index.IndexBuilderElementPath;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;
import org.apache.vxquery.runtime.functions.util.FunctionHelper;
import org.apache.vxquery.xmlparser.ITreeNodeIdProvider;
import org.apache.vxquery.xmlparser.TreeNodeIdProvider;
import org.xml.sax.InputSource;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.comm.util.ByteBufferInputStream;

public class IndexConstructorScalarEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public IndexConstructorScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        final UTF8StringPointable stringp = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        final TaggedValuePointable nodep = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        final ByteBufferInputStream bbis = new ByteBufferInputStream();
        final DataInputStream di = new DataInputStream(bbis);
        final SequenceBuilder sb = new SequenceBuilder();
        final ArrayBackedValueStorage abvsFileNode = new ArrayBackedValueStorage();
        final InputSource in = new InputSource();
        final int partition = ctx.getTaskAttemptId().getTaskId().getPartition();
        final ITreeNodeIdProvider nodeIdProvider = new TreeNodeIdProvider((short) partition);

        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            private boolean first = true;

            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                String collectionName;
                TaggedValuePointable tvp = args[0];

                if (tvp.getTag() != ValueTag.XS_STRING_TAG) {
                    throw new SystemException(ErrorCode.FORG0006);
                }
                tvp.getValue(stringp);
                try {
                    // Get the list of files.
                    bbis.setByteBuffer(ByteBuffer.wrap(Arrays.copyOfRange(stringp.getByteArray(),
                            stringp.getStartOffset(), stringp.getLength() + stringp.getStartOffset())), 0);
                    collectionName = di.readUTF();
                } catch (IOException e) {
                    throw new SystemException(ErrorCode.SYSE0001, e);
                }

                File collectionDirectory = new File(collectionName);
                if (!collectionDirectory.exists()) {
                    throw new RuntimeException("The collection directory (" + collectionName + ") does not exist.");
                }

                try {
                    abvs.reset();
                    sb.reset(abvs);
                    String indexPath = "/home/steven/vxquery/indexfolder";
                    System.out.println("Indexing to directory '" + indexPath + "'...");

                    Directory dir = FSDirectory.open(new File(indexPath));
                    Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_40);
                    IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_40, analyzer);

                    // Create will overwrite the index everytime

                    iwc.setOpenMode(OpenMode.CREATE);

                    //Create an index writer
                    IndexWriter writer = new IndexWriter(dir, iwc);

                    //Add files to index
                    indexXmlFiles(collectionDirectory, writer);

                    //This makes write slower but search faster.
                    writer.forceMerge(1);

                    writer.close();

                    sb.finish();
                    result.set(abvs);
                } catch (IOException e) {
                    throw new SystemException(ErrorCode.SYSE0001, e);
                }
            }

            /*This function goes recursively one file at a time. First it turns the file into an ABVS document node, then
             * it indexes that document node.
             */
            private void indexXmlFiles(File collectionDirectory, IndexWriter writer) throws SystemException,
                    IOException {
                for (File file : collectionDirectory.listFiles()) {

                    if (FunctionHelper.readableXmlFile(file.getPath())) {
                        abvsFileNode.reset();
                        // Get the document node
                        FunctionHelper.readInDocFromString(file.getPath(), in, abvsFileNode, nodeIdProvider);

                        nodep.set(abvsFileNode.getByteArray(), abvsFileNode.getStartOffset(), abvsFileNode.getLength());

                        //Add the document to the index
                        IndexBuilderElementPath ibuilder = new IndexBuilderElementPath(nodep, writer,
                                file.getAbsolutePath());

                        /*Output the names of the files being indexed
                        System.out.println("Indexing: " + file.getAbsolutePath());
                        */
                        ibuilder.printstart();

                        //This returns the first file that is parsed, so there is some XML output
                        //It basically shows one sample of the files that were indexed.
                        if (first) {
                            sb.addItem(nodep);
                            first = false;
                        }
                    } else if (file.isDirectory()) {
                        // Consider all XML file in sub directories.
                        indexXmlFiles(file, writer);
                    }
                }
            }
        };
    }
}