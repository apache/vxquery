package org.apache.vxquery.metadata;

import java.util.List;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.IPrinterFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSink;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSource;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSourceIndex;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IPushRuntimeFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.core.api.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.utils.Pair;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class VXQueryMetadataProvider implements IMetadataProvider<String, String> {
    @Override
    public IDataSource<String> findDataSource(String arg0) throws AlgebricksException {
        return null;
    }

    @Override
    public IDataSourceIndex<String, String> findDataSourceIndex(String arg0, String arg1) throws AlgebricksException {
        return null;
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getDeleteRuntime(IDataSource<String> arg0,
            IOperatorSchema arg1, List<LogicalVariable> arg2, LogicalVariable arg3, RecordDescriptor arg4,
            JobGenContext arg5, JobSpecification arg6) throws AlgebricksException {
        return null;
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getIndexDeleteRuntime(
            IDataSourceIndex<String, String> arg0, IOperatorSchema arg1, List<LogicalVariable> arg2,
            List<LogicalVariable> arg3, RecordDescriptor arg4, JobGenContext arg5, JobSpecification arg6)
            throws AlgebricksException {
        return null;
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getIndexInsertRuntime(
            IDataSourceIndex<String, String> arg0, IOperatorSchema arg1, List<LogicalVariable> arg2,
            List<LogicalVariable> arg3, RecordDescriptor arg4, JobGenContext arg5, JobSpecification arg6)
            throws AlgebricksException {
        return null;
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getInsertRuntime(IDataSource<String> arg0,
            IOperatorSchema arg1, List<LogicalVariable> arg2, LogicalVariable arg3, RecordDescriptor arg4,
            JobGenContext arg5, JobSpecification arg6) throws AlgebricksException {
        return null;
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getScannerRuntime(IDataSource<String> arg0,
            List<LogicalVariable> arg1, List<LogicalVariable> arg2, boolean arg3, JobGenContext arg4,
            JobSpecification arg5) throws AlgebricksException {
        return null;
    }

    @Override
    public Pair<IPushRuntimeFactory, AlgebricksPartitionConstraint> getWriteFileRuntime(IDataSink arg0, int[] arg1,
            IPrinterFactory[] arg2, RecordDescriptor arg3) throws AlgebricksException {
        return null;
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getWriteResultRuntime(IDataSource<String> arg0,
            IOperatorSchema arg1, List<LogicalVariable> arg2, LogicalVariable arg3, JobGenContext arg4,
            JobSpecification arg5) throws AlgebricksException {
        return null;
    }

    @Override
    public boolean scannerOperatorIsLeaf(IDataSource<String> arg0) {
        return false;
    }
}