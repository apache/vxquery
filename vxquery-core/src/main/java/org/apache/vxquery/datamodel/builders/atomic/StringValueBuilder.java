package org.apache.vxquery.datamodel.builders.atomic;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.hyracks.data.std.primitive.UTF8StringWriter;

public class StringValueBuilder {
    private final UTF8StringWriter writer;

    public StringValueBuilder() {
        writer = new UTF8StringWriter();
    }

    public void write(CharSequence string, DataOutput out) throws IOException {
        writer.writeUTF8String(string, out);
    }
}