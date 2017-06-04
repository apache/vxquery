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

import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.job.JobId;

/**
 * A class to map {@link ResultSetId} with {@link JobId} when a job is submitted
 * to hyracks. This mapping will later be used to determine the {@link JobId}
 * instance of the corresponding {@link ResultSetId}
 *
 * @author Erandi Ganepola
 */
public class HyracksJobContext {

    private JobId jobId;
    private int frameSize;
    private ResultSetId resultSetId;

    public HyracksJobContext(JobId jobId, int frameSize, ResultSetId resultSetId) {
        this.jobId = jobId;
        this.frameSize = frameSize;
        this.resultSetId = resultSetId;
    }

    public JobId getJobId() {
        return jobId;
    }

    public int getFrameSize() {
        return frameSize;
    }

    public ResultSetId getResultSetId() {
        return resultSetId;
    }
}
