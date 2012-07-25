package org.apache.vxquery.context;

import javax.xml.namespace.QName;

import org.apache.vxquery.types.SequenceType;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;

public class XQueryVariable {
    private final QName name;

    private final SequenceType type;

    private final LogicalVariable var;

    public XQueryVariable(QName name, SequenceType type, LogicalVariable var) {
        this.name = name;
        this.type = type;
        this.var = var;
    }

    public QName getName() {
        return name;
    }

    public SequenceType getType() {
        return type;
    }

    public LogicalVariable getLogicalVariable() {
        return var;
    }
}