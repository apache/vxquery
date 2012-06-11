package org.apache.vxquery.metadata;

import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSink;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.FileSplitDomain;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.RandomPartitioningProperty;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;

public class QueryResultDataSink implements IDataSink {
    private final FileSplit[] fileSplits;
    private final IPartitioningProperty pProperty;

    public QueryResultDataSink(FileSplit[] fileSplits) {
        this.fileSplits = fileSplits;
        pProperty = new RandomPartitioningProperty(new FileSplitDomain(fileSplits));
    }

    @Override
    public Object getId() {
        return null;
    }

    @Override
    public Object[] getSchemaTypes() {
        return null;
    }

    @Override
    public IPartitioningProperty getPartitioningProperty() {
        return pProperty;
    }

    public FileSplit[] getFileSplits() {
        return fileSplits;
    }
}