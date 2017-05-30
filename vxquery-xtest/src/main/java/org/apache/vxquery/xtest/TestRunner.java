/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.vxquery.xtest;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.client.NodeControllerInfo;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.dataset.DatasetJobRecord;
import org.apache.hyracks.api.dataset.IHyracksDataset;
import org.apache.hyracks.api.dataset.IHyracksDatasetReader;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.control.nc.resources.memory.FrameManager;
import org.apache.hyracks.dataflow.common.comm.io.ResultFrameTupleAccessor;
import org.apache.vxquery.compiler.CompilerControlBlock;
import org.apache.vxquery.compiler.algebricks.VXQueryGlobalDataFactory;
import org.apache.vxquery.context.DynamicContext;
import org.apache.vxquery.context.DynamicContextImpl;
import org.apache.vxquery.context.RootStaticContextImpl;
import org.apache.vxquery.context.StaticContextImpl;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.result.ResultUtils;
import org.apache.vxquery.xmlquery.query.VXQueryCompilationListener;
import org.apache.vxquery.xmlquery.query.XMLQueryCompiler;

public class TestRunner {
    private static final Pattern EMBEDDED_SYSERROR_PATTERN = Pattern.compile("(\\p{javaUpperCase}{4}\\d{4})");
    private List<String> collectionList;
    private XTestOptions opts;
    private IHyracksClientConnection hcc;
    private IHyracksDataset hds;

    public TestRunner(XTestOptions opts) throws UnknownHostException {
        this.opts = opts;
        this.collectionList = new ArrayList<String>();
    }

    public void open() throws Exception {
        hcc = TestClusterUtil.getConnection();
        hds = TestClusterUtil.getDataset();
    }

    protected static TestConfiguration getIndexConfiguration(TestCase testCase) {
        XTestOptions opts = new XTestOptions();
        opts.verbose = false;
        opts.threads = 1;
        opts.showQuery = true;
        opts.showResult = true;
        opts.hdfsConf = "src/test/resources/hadoop/conf";
        opts.catalog = StringUtils.join(new String[] { "src", "test", "resources", "VXQueryCatalog.xml" },
                File.separator);
        TestConfiguration indexConf = new TestConfiguration();
        indexConf.options = opts;
        String baseDir = new File(opts.catalog).getParent();
        try {
            String root = new File(baseDir).getCanonicalPath();
            indexConf.testRoot = new File(root + "/./");
            indexConf.resultOffsetPath = new File(root + "/./ExpectedResults/");
            indexConf.sourceFileMap = testCase.getSourceFileMap();
            indexConf.xqueryFileExtension = ".xq";
            indexConf.xqueryxFileExtension = "xqx";
            indexConf.xqueryQueryOffsetPath = new File(root + "/./Queries/XQuery/");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return indexConf;

    }

    public TestCaseResult run(final TestCase testCase) {
        TestCaseResult res = new TestCaseResult(testCase);
        TestCase testCaseIndex = new TestCase(getIndexConfiguration(testCase));
        testCaseIndex.setFolder("Indexing/Partition-1/");
        testCaseIndex.setName("showIndexes");
        runQuery(testCaseIndex, res);
        String[] collections = res.result.split("\n");
        this.collectionList = Arrays.asList(collections);
        runQueries(testCase, res);
        return res;
    }

    public void runQuery(TestCase testCase, TestCaseResult res) {
        if (opts.verbose) {
            System.err.println("Starting " + testCase.getXQueryDisplayName());
        }

        long start = System.currentTimeMillis();

        try {
            try {
                if (opts.showQuery) {

                    FileInputStream query = new FileInputStream(testCase.getXQueryFile());
                    System.err.println("***Query for " + testCase.getXQueryDisplayName() + ": ");
                    System.err.println(IOUtils.toString(query, "UTF-8"));
                    query.close();
                }

                VXQueryCompilationListener listener = new VXQueryCompilationListener(opts.showAST, opts.showTET,
                        opts.showOET, opts.showRP);

                Map<String, NodeControllerInfo> nodeControllerInfos = null;
                if (hcc != null) {
                    nodeControllerInfos = hcc.getNodeControllerInfos();
                }

                XMLQueryCompiler compiler = new XMLQueryCompiler(listener, nodeControllerInfos, opts.frameSize,
                        opts.hdfsConf);
                Reader in = new InputStreamReader(new FileInputStream(testCase.getXQueryFile()), "UTF-8");
                CompilerControlBlock ccb = new CompilerControlBlock(
                        new StaticContextImpl(RootStaticContextImpl.INSTANCE),
                        new ResultSetId(testCase.getXQueryDisplayName().hashCode()), testCase.getSourceFileMap());
                compiler.compile(testCase.getXQueryDisplayName(), in, ccb, opts.optimizationLevel, collectionList);
                JobSpecification spec = compiler.getModule().getHyracksJobSpecification();
                in.close();

                DynamicContext dCtx = new DynamicContextImpl(compiler.getModule().getModuleContext());
                spec.setGlobalJobDataFactory(new VXQueryGlobalDataFactory(dCtx.createFactory()));

                spec.setMaxReattempts(0);
                JobId jobId = hcc.startJob(spec, EnumSet.of(JobFlag.PROFILE_RUNTIME));

                FrameManager resultDisplayFrameMgr = new FrameManager(spec.getFrameSize());
                IFrame frame = new VSizeFrame(resultDisplayFrameMgr);
                IHyracksDatasetReader reader = hds.createReader(jobId, ccb.getResultSetId());
                // TODO(tillw) remove this loop once the IHyracksDatasetReader reliably returns the correct exception
                while (reader.getResultStatus() == DatasetJobRecord.Status.RUNNING) {
                    Thread.sleep(1);
                }
                IFrameTupleAccessor frameTupleAccessor = new ResultFrameTupleAccessor();
                res.result = "";
                while (reader.read(frame) > 0) {
                    res.result += ResultUtils.getStringFromBuffer(frame.getBuffer(), frameTupleAccessor);
                    frame.getBuffer().clear();
                }
                res.result.trim();
                hcc.waitForCompletion(jobId);
            } catch (HyracksException e) {
                Throwable t = e;
                while (t.getCause() != null) {
                    t = t.getCause();
                }
                final String message = t.getMessage();
                if (message != null) {
                    Matcher m = EMBEDDED_SYSERROR_PATTERN.matcher(message);
                    if (m.find()) {
                        String eCode = m.group(1);
                        throw new SystemException(ErrorCode.valueOf(eCode), e);
                    }
                }
                throw e;
            }
        } catch (Throwable e) {
            // Check for nested SystemExceptions.
            Throwable error = e;
            while (error != null) {
                if (error instanceof SystemException) {
                    res.error = error;
                    break;
                }
                error = error.getCause();
            }
            // Default
            if (res.error == null) {
                res.error = e;
            }
        } finally {
            try {
                res.compare();
            } catch (Exception e) {
                System.err.println("Framework error");
                e.printStackTrace();
            }
            long end = System.currentTimeMillis();
            res.time = end - start;
        }
        if (opts.showResult) {
            if (res.result == null) {
                System.err.println("***Error: ");
                System.err.println("Message: " + res.error.getMessage());
                res.error.printStackTrace();
            } else {
                System.err.println("***Result: ");
                System.err.println(res.result);
            }
        }

    }

    public void runQueries(TestCase testCase, TestCaseResult res) {
        runQuery(testCase, res);
    }

    public void close() throws Exception {
        // TODO add a close statement for the hyracks connection.
    }
}
