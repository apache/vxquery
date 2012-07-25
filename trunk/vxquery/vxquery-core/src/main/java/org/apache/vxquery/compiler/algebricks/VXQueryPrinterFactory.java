package org.apache.vxquery.compiler.algebricks;

import org.apache.vxquery.serializer.XMLSerializer;

import edu.uci.ics.hyracks.algebricks.data.IPrinter;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactory;

public class VXQueryPrinterFactory implements IPrinterFactory {
    private static final long serialVersionUID = 1L;

    @Override
    public IPrinter createPrinter() {
        return new XMLSerializer();
    }
}