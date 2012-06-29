package org.apache.vxquery.runtime.functions.node;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;
import org.apache.vxquery.xmlparser.XMLParser;
import org.xml.sax.InputSource;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;

public class FnDocScalarEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnDocScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IScalarEvaluator[] args) throws AlgebricksException {
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        final InputSource in = new InputSource();
        final UTF8StringPointable sp = new UTF8StringPointable();
        final ByteBufferInputStream bbis = new ByteBufferInputStream();
        final DataInputStream di = new DataInputStream(bbis);
        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                try {
                    TaggedValuePointable tvp = args[0];
                    if (tvp.getTag() != ValueTag.XS_STRING_TAG) {
                        throw new SystemException(ErrorCode.FORG0006);
                    }
                    tvp.getValue(sp);
                    bbis.setByteBuffer(
                            ByteBuffer.wrap(Arrays.copyOfRange(sp.getByteArray(), sp.getStartOffset(), sp.getLength()
                                    + sp.getStartOffset())), 0);
                    String fName = di.readUTF();
                    in.setCharacterStream(new InputStreamReader(new FileInputStream(fName)));
                    XMLParser.parseInputSource(in, abvs, false, null);
                    result.set(abvs);
                } catch (IOException e) {
                    throw new SystemException(ErrorCode.SYSE0001, e);
                }
            }
        };
    }
}