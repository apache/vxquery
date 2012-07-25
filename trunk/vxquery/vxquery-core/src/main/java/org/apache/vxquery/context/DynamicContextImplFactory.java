package org.apache.vxquery.context;

import java.util.Arrays;
import java.util.Map;

import javax.xml.namespace.QName;

import edu.uci.ics.hyracks.api.context.IHyracksJobletContext;
import edu.uci.ics.hyracks.data.std.primitive.VoidPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

class DynamicContextImplFactory implements IDynamicContextFactory {
    private static final long serialVersionUID = 1L;

    private final IStaticContextFactory scFactory;

    private final byte[] currentDateTime;

    private final QName[] variableNames;

    private final int[] valueOffsets;

    private final byte[] variableValues;

    private DynamicContextImplFactory(IStaticContextFactory scFactory, byte[] currentDateTime, QName[] variableNames,
            int[] valueOffsets, byte[] variableValues) {
        this.scFactory = scFactory;
        this.currentDateTime = currentDateTime;
        this.variableNames = variableNames;
        this.valueOffsets = valueOffsets;
        this.variableValues = variableValues;
    }

    @Override
    public DynamicContext createDynamicContext(IHyracksJobletContext ctx) {
        StaticContext sCtx = scFactory.createStaticContext();
        DynamicContextImpl dCtx = new DynamicContextImpl(sCtx);
        VoidPointable vp = new VoidPointable();
        vp.set(currentDateTime, 0, currentDateTime.length);
        dCtx.setCurrentDateTime(vp);
        for (int i = 0; i < variableNames.length; ++i) {
            QName vName = variableNames[i];
            int vStart = i == 0 ? 0 : valueOffsets[i - 1];
            int vEnd = valueOffsets[i];
            vp.set(variableValues, vStart, vEnd - vStart);
            dCtx.bindVariable(vName, vp);
        }
        return dCtx;
    }

    static IDynamicContextFactory createInstance(DynamicContextImpl dCtx) {
        IStaticContextFactory scFactory = dCtx.getStaticContext().createFactory();
        VoidPointable vp = new VoidPointable();
        dCtx.getCurrentDateTime(vp);
        byte[] currentDateTime = Arrays.copyOfRange(vp.getByteArray(), vp.getStartOffset(), vp.getLength());

        Map<QName, ArrayBackedValueStorage> vMap = dCtx.getVariableMap();
        int nVars = vMap.size();
        QName[] variableNames = new QName[nVars];
        int[] valueOffsets = new int[nVars];
        ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        int i = 0;
        for (Map.Entry<QName, ArrayBackedValueStorage> e : vMap.entrySet()) {
            variableNames[i] = e.getKey();
            abvs.append(e.getValue());
            valueOffsets[i] = abvs.getLength();
            ++i;
        }

        return new DynamicContextImplFactory(scFactory, currentDateTime, variableNames, valueOffsets,
                Arrays.copyOfRange(abvs.getByteArray(), abvs.getStartOffset(), abvs.getLength()));
    }
}