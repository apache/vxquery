package org.apache.vxquery.compiler.algebricks;

import edu.uci.ics.hyracks.algebricks.core.algebra.data.IPrinterFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.IPrinterFactoryProvider;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;

public class PrinterFactoryProvider implements IPrinterFactoryProvider {
    public static final IPrinterFactoryProvider INSTANCE = new PrinterFactoryProvider();

    @Override
    public IPrinterFactory getPrinterFactory(Object arg0) throws AlgebricksException {
        return null;
    }
}