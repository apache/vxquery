package org.apache.vxquery.xmlquery.translator;

import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.vxquery.datamodel.values.ValueTag;

import edu.uci.ics.hyracks.algebricks.runtime.base.IUnnestingPositionWriter;

public class VXQueryPositionWriter implements IUnnestingPositionWriter, Serializable {
    private static final long serialVersionUID = 1L;

    public void write(DataOutput dataOutput, int position) throws IOException {
        dataOutput.writeByte(ValueTag.XS_INTEGER_TAG);
        dataOutput.writeLong(position);
    }

}
