package org.apache.vxquery.metadata;

import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSource;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSourcePropertiesProvider;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.RandomPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.vxquery.compiler.rewriter.rules.CollectionFileDomain;

import java.util.ArrayList;
import java.util.List;

/**
 * Datasource object for indexing.
 */
public class VXQueryIndexingDatasource implements IDataSource<String> {
    private static final String DELIMITER = "\\|";
    private final int dataSourceId;
    private final String collectionName;
    private final String indexName;
    private String[] collectionPartitions;
    private String[] indexPartitions;
    private final List<Integer> childSeq;
    private int totalDataSources;
    private String tag;

    private final Object[] types;

    private IDataSourcePropertiesProvider propProvider;

    private VXQueryIndexingDatasource(int id, String collection, String index, Object[] types) {
        this.dataSourceId = id;
        this.collectionName = collection;
        this.indexName = index;
        collectionPartitions = collectionName.split(DELIMITER);
        indexPartitions = indexName.split(DELIMITER);
        this.types = types;
        final IPhysicalPropertiesVector vec = new StructuralPropertiesVector(
                new RandomPartitioningProperty(new CollectionFileDomain(collectionName)),
                new ArrayList<ILocalStructuralProperty>());
        propProvider = new IDataSourcePropertiesProvider() {
            @Override
            public IPhysicalPropertiesVector computePropertiesVector(List<LogicalVariable> scanVariables) {
                return vec;
            }
        };
        this.childSeq = new ArrayList<>();
        this.tag = null;
    }

    public static VXQueryIndexingDatasource create(int id, String collection, String index, Object type) {
        return new VXQueryIndexingDatasource(id, collection, index, new Object[] { type });
    }

    public int getTotalDataSources() {
        return totalDataSources;
    }

    public void setTotalDataSources(int totalDataSources) {
        this.totalDataSources = totalDataSources;
    }

    public int getDataSourceId() {
        return dataSourceId;
    }

    public String[] getCollectionPartitions() {
        return collectionPartitions;
    }

    public void setPartitions(String[] collectionPartitions) {
        this.collectionPartitions = collectionPartitions;
    }

    public int getPartitionCount() {
        return collectionPartitions.length;
    }

    public String getTag() {
        return this.tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public void addChildSeq(int integer) {
        childSeq.add(integer);
    }

    public List<Integer> getChildSeq() {
        return childSeq;
    }

    public String[] getIndexPartitions() {
        return indexPartitions;
    }

    public void setIndexPartitions(String[] indexPartitions) {
        this.indexPartitions = indexPartitions;
    }

    @Override
    public String getId() {
        return null;
    }

    @Override
    public Object[] getSchemaTypes() {
        return new Object[0];
    }

    @Override
    public IDataSourcePropertiesProvider getPropertiesProvider() {
        return null;
    }

    @Override
    public void computeFDs(List scanVariables, List fdList) {

    }

    @Override
    public String toString() {
        return "VXQueryIndexingDataSource [collectionName=" + collectionName + ", indexName=" +  "childSeq=" +
                childSeq + "]";
    }
}
