package org.apache.vxquery.metadata;

import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSourcePropertiesProvider;

import java.util.List;

public abstract class AbstractVXQueryDataSource {
    protected static final String DELIMITER = "\\|";
    protected int dataSourceId;
    protected String collectionName;
    protected String[] collectionPartitions;
    protected String elementPath;
    protected List<Integer> childSeq;
    protected int totalDataSources;
    protected String tag;
    protected String function;

    protected Object[] types;

    protected IDataSourcePropertiesProvider propProvider;

    public abstract String getDataSourceType();
}
