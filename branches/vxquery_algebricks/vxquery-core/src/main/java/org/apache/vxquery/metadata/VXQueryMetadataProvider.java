package org.apache.vxquery.metadata;

import java.util.List;

import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSink;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSource;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSourceIndex;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class VXQueryMetadataProvider implements IMetadataProvider<String, String> {
    @Override
    public IDataSource<String> findDataSource(String id) throws AlgebricksException {
        return null;
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getScannerRuntime(IDataSource<String> dataSource,
            List<LogicalVariable> scanVariables, List<LogicalVariable> projectVariables, boolean projectPushed,
            JobGenContext context, JobSpecification jobSpec) throws AlgebricksException {
        return null;
    }

    @Override
    public boolean scannerOperatorIsLeaf(IDataSource<String> dataSource) {
        return false;
    }

    @Override
    public Pair<IPushRuntimeFactory, AlgebricksPartitionConstraint> getWriteFileRuntime(IDataSink sink,
            int[] printColumns, IPrinterFactory[] printerFactories, RecordDescriptor inputDesc)
            throws AlgebricksException {
        return null;
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getWriteResultRuntime(
            IDataSource<String> dataSource, IOperatorSchema propagatedSchema, List<LogicalVariable> keys,
            LogicalVariable payLoadVar, JobGenContext context, JobSpecification jobSpec) throws AlgebricksException {
        return null;
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getInsertRuntime(IDataSource<String> dataSource,
            IOperatorSchema propagatedSchema, List<LogicalVariable> keys, LogicalVariable payLoadVar,
            RecordDescriptor recordDesc, JobGenContext context, JobSpecification jobSpec) throws AlgebricksException {
        return null;
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getDeleteRuntime(IDataSource<String> dataSource,
            IOperatorSchema propagatedSchema, List<LogicalVariable> keys, LogicalVariable payLoadVar,
            RecordDescriptor recordDesc, JobGenContext context, JobSpecification jobSpec) throws AlgebricksException {
        return null;
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getIndexInsertRuntime(
            IDataSourceIndex<String, String> dataSource, IOperatorSchema propagatedSchema,
            IOperatorSchema[] inputSchemas, IVariableTypeEnvironment typeEnv, List<LogicalVariable> primaryKeys,
            List<LogicalVariable> secondaryKeys, ILogicalExpression filterExpr, RecordDescriptor recordDesc,
            JobGenContext context, JobSpecification spec) throws AlgebricksException {
        return null;
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getIndexDeleteRuntime(
            IDataSourceIndex<String, String> dataSource, IOperatorSchema propagatedSchema,
            IOperatorSchema[] inputSchemas, IVariableTypeEnvironment typeEnv, List<LogicalVariable> primaryKeys,
            List<LogicalVariable> secondaryKeys, ILogicalExpression filterExpr, RecordDescriptor recordDesc,
            JobGenContext context, JobSpecification spec) throws AlgebricksException {
        return null;
    }

    @Override
    public IDataSourceIndex<String, String> findDataSourceIndex(String indexId, String dataSourceId)
            throws AlgebricksException {
        return null;
    }

    @Override
    public IFunctionInfo lookupFunction(FunctionIdentifier fid) {
        return null;
    }
}