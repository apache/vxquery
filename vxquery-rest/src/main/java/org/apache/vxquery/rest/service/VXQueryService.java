/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.vxquery.rest.service;

import static java.util.logging.Level.SEVERE;
import static org.apache.vxquery.rest.Constants.ErrorCodes.NOT_FOUND;
import static org.apache.vxquery.rest.Constants.ErrorCodes.PROBLEM_WITH_QUERY;
import static org.apache.vxquery.rest.Constants.ErrorCodes.UNFORSEEN_PROBLEM;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.AlgebricksAppendable;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.LogicalOperatorPrettyPrintVisitor;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.PlanPrettyPrinter;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionVisitor;
import org.apache.hyracks.api.client.HyracksConnection;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.client.NodeControllerInfo;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.dataset.DatasetJobRecord;
import org.apache.hyracks.api.dataset.IHyracksDatasetReader;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.client.dataset.HyracksDataset;
import org.apache.hyracks.control.nc.resources.memory.FrameManager;
import org.apache.hyracks.dataflow.common.comm.io.ResultFrameTupleAccessor;
import org.apache.vxquery.compiler.CompilerControlBlock;
import org.apache.vxquery.compiler.algebricks.VXQueryGlobalDataFactory;
import org.apache.vxquery.compiler.algebricks.prettyprint.VXQueryLogicalExpressionPrettyPrintVisitor;
import org.apache.vxquery.context.DynamicContext;
import org.apache.vxquery.context.DynamicContextImpl;
import org.apache.vxquery.context.RootStaticContextImpl;
import org.apache.vxquery.context.StaticContextImpl;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.exceptions.VXQueryRuntimeException;
import org.apache.vxquery.rest.request.QueryRequest;
import org.apache.vxquery.rest.request.QueryResultRequest;
import org.apache.vxquery.rest.response.APIResponse;
import org.apache.vxquery.rest.response.Error;
import org.apache.vxquery.rest.response.QueryResponse;
import org.apache.vxquery.rest.response.QueryResultResponse;
import org.apache.vxquery.rest.response.SyncQueryResponse;
import org.apache.vxquery.result.ResultUtils;
import org.apache.vxquery.xmlquery.ast.ModuleNode;
import org.apache.vxquery.xmlquery.query.Module;
import org.apache.vxquery.xmlquery.query.XMLQueryCompiler;
import org.apache.vxquery.xmlquery.query.XQueryCompilationListener;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.DomDriver;

/**
 * Main class responsible for handling query requests. This class will first
 * compile, then submit query to hyracks and finally fetch results for a given
 * query.
 *
 * @author Erandi Ganepola
 */
public class VXQueryService {

    private static final Logger LOGGER = Logger.getLogger(VXQueryService.class.getName());

    private static final Pattern EMBEDDED_SYSERROR_PATTERN = Pattern.compile("(\\p{javaUpperCase}{4}\\d{4})");

    private volatile State state = State.STOPPED;
    private VXQueryConfig vxQueryConfig;
    private AtomicLong atomicLong = new AtomicLong(0);
    private Map<Long, HyracksJobContext> jobContexts = new ConcurrentHashMap<>();
    private IHyracksClientConnection hyracksClientConnection;
    private HyracksDataset hyracksDataset;

    public VXQueryService(VXQueryConfig config) {
        vxQueryConfig = config;
    }

    /**
     * Starts VXQueryService class by creating a {@link IHyracksClientConnection}
     * which will later be used to submit and retrieve queries and results to/from
     * hyracks.
     */
    public synchronized void start() {
        if (!State.STOPPED.equals(state)) {
            throw new IllegalStateException("VXQueryService is at state : " + state);
        }

        if (vxQueryConfig.getHyracksClientIp() == null) {
            throw new IllegalArgumentException("hyracksClientIp is required to connect to hyracks");
        }

        setState(State.STARTING);

        try {
            hyracksClientConnection = new HyracksConnection(vxQueryConfig.getHyracksClientIp(),
                    vxQueryConfig.getHyracksClientPort());
        } catch (Exception e) {
            LOGGER.log(SEVERE, String.format("Unable to create a hyracks client connection to %s:%d",
                    vxQueryConfig.getHyracksClientIp(), vxQueryConfig.getHyracksClientPort()));
            throw new VXQueryRuntimeException("Unable to create a hyracks client connection", e);
        }

        LOGGER.log(Level.FINE, String.format("Using hyracks connection to %s:%d", vxQueryConfig.getHyracksClientIp(),
                vxQueryConfig.getHyracksClientPort()));

        setState(State.STARTED);
        LOGGER.log(Level.INFO, "VXQueryService started successfully");
    }

    private synchronized void setState(State newState) {
        state = newState;
    }

    /**
     * Submits a query to hyracks to be run after compiling. Required intermediate
     * results and metrics are also calculated according to the
     * {@link QueryRequest}. Checks if this class has started before moving further.
     *
     * @param request
     *            {@link QueryRequest} containing information about the query to be
     *            executed and the merics required along with the results
     * @return AsyncQueryResponse if no error occurs | ErrorResponse else
     */
    public APIResponse execute(final QueryRequest request) {
        List<String> collections = new ArrayList<>();
//        if (request.useIndexing()) {
//            QueryRequest indexingRequest = new QueryRequest("show-indexes()");
//            indexingRequest.setAsync(false);
//            SyncQueryResponse indexingResponse = (SyncQueryResponse) execute(indexingRequest, new ArrayList<>());
//            LOGGER.log(Level.FINE, String.format("Found indexes: %s", indexingResponse.getResults()));
//
//            collections = Arrays.asList(indexingResponse.getResults().split("\n"));
//        }
        return execute(request, collections);
    }

    private APIResponse execute(final QueryRequest request, List<String> collections) {
        if (!State.STARTED.equals(state)) {
            throw new IllegalStateException("VXQueryService is at state : " + state);
        }

        String query = request.getStatement();
        final ResultSetId resultSetId = createResultSetId();

        QueryResponse response = APIResponse.newQueryResponse(request, resultSetId);
        response.setStatement(query);

        // Obtaining the node controller information from hyracks client connection
        Map<String, NodeControllerInfo> nodeControllerInfos = null;
        try {
            nodeControllerInfos = hyracksClientConnection.getNodeControllerInfos();
        } catch (HyracksException e) {
            LOGGER.log(Level.SEVERE, String.format("Error occurred when obtaining NC info: '%s'", e.getMessage()));
            return APIResponse.newErrorResponse(request.getRequestId(), Error.builder().withCode(UNFORSEEN_PROBLEM)
                    .withMessage("Hyracks connection problem: " + e.getMessage()).build());
        }

        // Adding a query compilation listener
        VXQueryCompilationListener listener = new VXQueryCompilationListener(response,
                request.isShowAbstractSyntaxTree(), request.isShowTranslatedExpressionTree(),
                request.isShowOptimizedExpressionTree(), request.isShowRuntimePlan());

        Date start = new Date();
        // Compiling the XQuery given
        final XMLQueryCompiler compiler = new XMLQueryCompiler(listener, nodeControllerInfos, request.getFrameSize(),
                vxQueryConfig.getAvailableProcessors(), vxQueryConfig.getJoinHashSize(),
                vxQueryConfig.getMaximumDataSize(), vxQueryConfig.getHdfsConf());
        CompilerControlBlock compilerControlBlock = new CompilerControlBlock(
                new StaticContextImpl(RootStaticContextImpl.INSTANCE), resultSetId, request.getSourceFileMap());
        try {
            compiler.compile(null, new StringReader(query), compilerControlBlock, request.getOptimization(),
                    collections);
        } catch (AlgebricksException e) {
            LOGGER.log(Level.SEVERE, String.format("Error occurred when compiling query: '%s' with message: '%s'",
                    query, e.getMessage()));
            return APIResponse.newErrorResponse(request.getRequestId(), Error.builder().withCode(PROBLEM_WITH_QUERY)
                    .withMessage("Query compilation failure: " + e.getMessage()).build());
        } catch (SystemException e) {
            LOGGER.log(Level.SEVERE, String.format("Error occurred when compiling query: '%s' with message: '%s'",
                    query, e.getMessage()));
            return APIResponse.newErrorResponse(request.getRequestId(),
                    new Error(PROBLEM_WITH_QUERY, "Query compilation failure: " + e.getCode()));
        }

        if (request.isShowMetrics()) {
            response.getMetrics().setCompileTime(new Date().getTime() - start.getTime());
        }

        if (request.isCompileOnly()) {
            return response;
        }

        Module module = compiler.getModule();
        JobSpecification js = module.getHyracksJobSpecification();
        DynamicContext dCtx = new DynamicContextImpl(module.getModuleContext());
        js.setGlobalJobDataFactory(new VXQueryGlobalDataFactory(dCtx.createFactory()));

        HyracksJobContext hyracksJobContext;
        start = new Date();
        if (!request.isAsync()) {
            for (int i = 0; i < request.getRepeatExecutions(); i++) {
                try {
                    hyracksJobContext = executeJob(js, resultSetId, request);

                } catch (Exception e) {
                    LOGGER.log(SEVERE, "Error occurred when submitting job to hyracks for query: " + query, e);
                    return APIResponse.newErrorResponse(request.getRequestId(),
                            Error.builder().withCode(UNFORSEEN_PROBLEM)
                                    .withMessage("Error occurred when starting hyracks job").build());
                }
                try {
                    String results = readResults(hyracksJobContext);
                    ((SyncQueryResponse) response).setResults(results);
                } catch (HyracksException e) {
                    LOGGER.log(Level.SEVERE, "Error occurred when reading results", e);
                    SystemException se = getSystemException(e);
                    return APIResponse.newErrorResponse(request.getRequestId(), new Error(UNFORSEEN_PROBLEM,
                            String.format("Error occurred when reading results: %s", se != null ? se.getCode() : "")));
                } catch (Exception e) {
                    LOGGER.log(Level.SEVERE, "Error occurred when reading results", e);
                    return APIResponse.newErrorResponse(request.getRequestId(),
                            new Error(UNFORSEEN_PROBLEM, "Error occurred when reading results: " + e.getMessage()));
                }
            }
        } else {
            try {
                hyracksJobContext = executeJob(js, resultSetId, request);
            } catch (Exception e) {
                LOGGER.log(SEVERE, "Error occurred when submitting job to hyracks for query: " + query, e);
                return APIResponse.newErrorResponse(request.getRequestId(), Error.builder().withCode(UNFORSEEN_PROBLEM)
                        .withMessage("Error occurred when starting hyracks job").build());
            }
            jobContexts.put(resultSetId.getId(), hyracksJobContext);
        }

        if (request.isShowMetrics()) {
            response.getMetrics().setElapsedTime(new Date().getTime() - start.getTime());
        }

        return response;
    }

    private HyracksJobContext executeJob(JobSpecification js, ResultSetId resultSetId, QueryRequest request)
            throws Exception {
        HyracksJobContext hyracksJobContext;
        JobId jobId = hyracksClientConnection.startJob(js, EnumSet.of(JobFlag.PROFILE_RUNTIME));
        hyracksJobContext = new HyracksJobContext(jobId, js.getFrameSize(), resultSetId);

        return hyracksJobContext;
    }

    private static SystemException getSystemException(HyracksException e) {
        Throwable t = e;
        Throwable candidate = t instanceof SystemException ? t : null;
        while (t.getCause() != null) {
            t = t.getCause();
            if (t instanceof SystemException) {
                candidate = t;
            }
        }

        t = candidate == null ? t : candidate;
        final String message = t.getMessage();
        if (message != null) {
            Matcher m = EMBEDDED_SYSERROR_PATTERN.matcher(message);
            if (m.find()) {
                String eCode = m.group(1);
                return new SystemException(ErrorCode.valueOf(eCode), e);
            }
        }
        return null;
    }

    /**
     * Returns the query results for a given result set id.
     *
     * @param request
     *            {@link QueryResultRequest} with result ID required
     * @return Either a {@link QueryResultResponse} if no error occurred |
     *         {@link org.apache.vxquery.rest.response.ErrorResponse} else.
     */
    public APIResponse getResult(QueryResultRequest request) {
        if (jobContexts.containsKey(request.getResultId())) {
            QueryResultResponse resultResponse = APIResponse.newQueryResultResponse(request.getRequestId());
            Date start = new Date();
            try {
                String results = readResults(jobContexts.get(request.getResultId()));
                resultResponse.setResults(results);
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error occurred when reading results for id : " + request.getResultId());
                return APIResponse.newErrorResponse(request.getRequestId(), new Error(UNFORSEEN_PROBLEM,
                        "Error occurred when reading results for: " + request.getResultId()));
            }

            if (request.isShowMetrics()) {
                resultResponse.getMetrics().setElapsedTime(new Date().getTime() - start.getTime());
            }

            return resultResponse;
        } else {
            return APIResponse.newErrorResponse(request.getRequestId(), Error.builder().withCode(NOT_FOUND)
                    .withMessage("No query found for result ID : " + request.getResultId()).build());
        }
    }

    /**
     * Reads results from hyracks given the {@link HyracksJobContext} containing
     * {@link ResultSetId} and {@link JobId} mapping.
     *
     * @param jobContext
     *            mapoing between the {@link ResultSetId} and corresponding hyracks
     *            {@link JobId}
     * @return Results of the given query
     * @throws Exception
     *             IOErrors and etc
     */
    private String readResults(HyracksJobContext jobContext) throws Exception {
        int nReaders = 1;

        if (hyracksDataset == null) {
            hyracksDataset = new HyracksDataset(hyracksClientConnection, jobContext.getFrameSize(), nReaders);
        }

        FrameManager resultDisplayFrameMgr = new FrameManager(jobContext.getFrameSize());
        IFrame frame = new VSizeFrame(resultDisplayFrameMgr);
        IHyracksDatasetReader reader = hyracksDataset.createReader(jobContext.getJobId(), jobContext.getResultSetId());
        OutputStream resultStream = new ByteArrayOutputStream();

        // This loop is required for XTests to reliably identify the error code of
        // SystemException.
        while (reader.getResultStatus().getState() == DatasetJobRecord.State.RUNNING) {
            Thread.sleep(100);
        }

        IFrameTupleAccessor frameTupleAccessor = new ResultFrameTupleAccessor();
        try (PrintWriter writer = new PrintWriter(resultStream, true)) {
            while (reader.read(frame) > 0) {
                writer.print(ResultUtils.getStringFromBuffer(frame.getBuffer(), frameTupleAccessor));
                writer.flush();
                frame.getBuffer().clear();
            }
        }

        hyracksClientConnection.waitForCompletion(jobContext.getJobId());
        LOGGER.log(Level.FINE, String.format("Result for resultId %d completed", jobContext.getResultSetId().getId()));
        return resultStream.toString();
    }

    /**
     * Create a unique result set id to get the correct query back from the cluster.
     *
     * @return Result Set id generated with current system time.
     */
    protected ResultSetId createResultSetId() {
        long resultSetId = atomicLong.incrementAndGet();
        LOGGER.log(Level.FINE, String.format("Creating result set with ID : %d", resultSetId));
        return new ResultSetId(resultSetId);
    }

    public synchronized void stop() {
        if (!State.STOPPED.equals(state)) {
            setState(State.STOPPING);
            LOGGER.log(Level.FINE, "Stooping VXQueryService");
            setState(State.STOPPED);
            LOGGER.log(Level.INFO, "VXQueryService stopped successfully");
        } else {
            LOGGER.log(Level.INFO, "VXQueryService is already in state : " + state);
        }
    }

    public State getState() {
        return state;
    }

    /**
     * A {@link XQueryCompilationListener} implementation to be used to add
     * AbstractSyntaxTree, RuntimePlan and etc to the {@link QueryResponse} if
     * requested by the user.
     */
    private class VXQueryCompilationListener implements XQueryCompilationListener {
        private QueryResponse response;
        private boolean showAbstractSyntaxTree;
        private boolean showTranslatedExpressionTree;
        private boolean showOptimizedExpressionTree;
        private boolean showRuntimePlan;

        public VXQueryCompilationListener(QueryResponse response, boolean showAbstractSyntaxTree,
                boolean showTranslatedExpressionTree, boolean showOptimizedExpressionTree, boolean showRuntimePlan) {
            this.response = response;
            this.showAbstractSyntaxTree = showAbstractSyntaxTree;
            this.showTranslatedExpressionTree = showTranslatedExpressionTree;
            this.showOptimizedExpressionTree = showOptimizedExpressionTree;
            this.showRuntimePlan = showRuntimePlan;
        }

        @Override
        public void notifyParseResult(ModuleNode moduleNode) {
            if (showAbstractSyntaxTree) {
                response.setAbstractSyntaxTree(new XStream(new DomDriver()).toXML(moduleNode));
            }
        }

        @Override
        public void notifyTranslationResult(Module module) {
            if (showTranslatedExpressionTree) {
                response.setTranslatedExpressionTree(appendPrettyPlan(new StringBuilder(), module).toString());
            }
        }

        @Override
        public void notifyTypecheckResult(Module module) {
        }

        @Override
        public void notifyCodegenResult(Module module) {
            if (showRuntimePlan) {
                JobSpecification jobSpec = module.getHyracksJobSpecification();
                try {
                    response.setRuntimePlan(jobSpec.toJSON().toString());
                } catch (IOException e) {
                    LOGGER.log(SEVERE,
                            "Error occurred when obtaining runtime plan from job specification : " + jobSpec.toString(),
                            e);
                }
            }
        }

        @Override
        public void notifyOptimizedResult(Module module) {
            if (showOptimizedExpressionTree) {
                response.setOptimizedExpressionTree(appendPrettyPlan(new StringBuilder(), module).toString());
            }
        }

        @SuppressWarnings("Duplicates")
        private StringBuilder appendPrettyPlan(StringBuilder sb, Module module) {
            try {
                ILogicalExpressionVisitor<String, Integer> ev = new VXQueryLogicalExpressionPrettyPrintVisitor(
                        module.getModuleContext());
                AlgebricksAppendable buffer = new AlgebricksAppendable();
                LogicalOperatorPrettyPrintVisitor v = new LogicalOperatorPrettyPrintVisitor(buffer, ev);
                PlanPrettyPrinter.printPlan(module.getBody(), v, 0);
                sb.append(buffer.toString());
            } catch (AlgebricksException e) {
                LOGGER.log(SEVERE, "Error occurred when pretty printing expression : " + e.getMessage());
            }
            return sb;
        }
    }
}
