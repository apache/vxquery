package org.apache.vxquery.compiler.algebricks;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.datamodel.values.XDMConstants;

import edu.uci.ics.hyracks.api.dataflow.value.INullWriter;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriterFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.primitive.VoidPointable;

public class VXQueryNullWriterFactory implements INullWriterFactory {
    private static final long serialVersionUID = 1L;

    @Override
    public INullWriter createNullWriter() {
        final VoidPointable vp = (VoidPointable) VoidPointable.FACTORY.createPointable();
        return new INullWriter() {
            @Override
            public void writeNull(DataOutput out) throws HyracksDataException {
                XDMConstants.setEmptySequence(vp);
                try {
                    out.write(vp.getByteArray(), vp.getStartOffset(), vp.getLength());
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
        };
    }
}