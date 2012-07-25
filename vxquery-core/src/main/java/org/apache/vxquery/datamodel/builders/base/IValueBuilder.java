package org.apache.vxquery.datamodel.builders.base;

import java.io.DataOutput;

public interface IValueBuilder {
    public void reset();

    public void write(DataOutput out);
}