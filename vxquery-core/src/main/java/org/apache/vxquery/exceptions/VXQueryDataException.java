/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.vxquery.exceptions;

import java.io.File;

import org.apache.hyracks.api.exceptions.HyracksDataException;

public class VXQueryDataException extends HyracksDataException {

    private static final long serialVersionUID = 1L;

    private File file;

    public VXQueryDataException(String message, Exception ex, File file, String nodeId) {
        super(message, ex, nodeId);
        this.file = file;
    }

    @Override
    public String getMessage() {
        String message = super.getMessage();
        message = message.replaceAll("\\[nodeId\\]", getNodeId());
        message = message.replaceAll("\\[path\\]", file.getAbsolutePath());
        return message;
    }
}
