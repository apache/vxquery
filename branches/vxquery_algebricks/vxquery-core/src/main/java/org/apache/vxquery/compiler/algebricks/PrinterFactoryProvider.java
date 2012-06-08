package org.apache.vxquery.compiler.algebricks;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactory;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactoryProvider;

public class PrinterFactoryProvider implements IPrinterFactoryProvider {
    public static final IPrinterFactoryProvider INSTANCE = new PrinterFactoryProvider();

    @Override
    public IPrinterFactory getPrinterFactory(Object arg0) throws AlgebricksException {
        return null;
    }
}