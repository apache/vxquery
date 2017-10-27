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

package org.apache.vxquery.rest;

public class Constants {

    private Constants() {
    }

    public class Parameters {
        public static final String STATEMENT = "statement";
        public static final String RESULT_ID = "resultId ";
        public static final String COMPILE_ONLY = "compileOnly";
        public static final String OPTIMIZATION = "optimization";
        public static final String FRAME_SIZE = "frameSize";
        public static final String REPEAT_EXECUTIONS = "repeatExecutions";
        public static final String METRICS = "metrics";
        public static final String SHOW_AST = "showAbstractSyntaxTree";
        public static final String SHOW_TET = "showTranslatedExpressionTree";
        public static final String SHOW_OET = "showOptimizedExpressionTree";
        public static final String SHOW_RP = "showRuntimePlan";
        public static final String USE_INDEX = "useIndexing";
        public static final String MODE = "mode";
    }

    public class URLs {
        public static final String BASE_PATH = "/vxquery";

        public static final String QUERY_ENDPOINT = BASE_PATH + "/query";
        public static final String QUERY_RESULT_ENDPOINT = BASE_PATH + "/query/result/*";
    }

    public class Properties {
        public static final String AVAILABLE_PROCESSORS = "org.apache.vxquery.available_processors";
        public static final String LOCAL_NODE_CONTROLLERS = "org.apache.vxquery.local_nc";
        public static final String JOIN_HASH_SIZE = "org.apache.vxquery.join_hash";
        public static final String MAXIMUM_DATA_SIZE = "org.apache.vxquery.data_size";
        public static final String HDFS_CONFIG = "org.apache.vxquery.hdfs_config";
    }

    public class HttpHeaderValues {
        public static final String CONTENT_TYPE_JSON = "application/json";
        public static final String CONTENT_TYPE_XML = "application/xml";
    }

    public class ErrorCodes {
        public static final int PROBLEM_WITH_QUERY = 400;
        public static final int UNFORSEEN_PROBLEM = 500;
        public static final int INVALID_INPUT = 405;
        public static final int NOT_FOUND = 404;
    }

    public static final String RESULT_URL_PREFIX = "/vxquery/query/result/";

    public static final String MODE_ASYNC = "async";
    public static final String MODE_SYNC = "sync";
}
