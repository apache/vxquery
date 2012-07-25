package org.apache.vxquery.compiler.algebricks;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactory;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactoryProvider;

public class VXQueryPrinterFactoryProvider implements IPrinterFactoryProvider {
    public static final IPrinterFactoryProvider INSTANCE = new VXQueryPrinterFactoryProvider();

    @Override
    public IPrinterFactory getPrinterFactory(Object type) throws AlgebricksException {

        return new VXQueryPrinterFactory();
    }
}