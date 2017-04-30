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
package org.apache.vxquery.types;

import java.util.Arrays;

import org.apache.hyracks.util.string.UTF8StringUtil;

public final class ProcessingInstructionType extends AbstractNodeType {
    private static final long serialVersionUID = 1L;

    public static final ProcessingInstructionType ANYPI = new ProcessingInstructionType(null);

    private byte[] target;

    public ProcessingInstructionType(byte[] target) {
        this.target = target;
    }

    @Override
    public NodeKind getNodeKind() {
        return NodeKind.PI;
    }

    public byte[] getTarget() {
        return target;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("processing-instruction(");
        if (target != null) {
            UTF8StringUtil.toString(sb, target, 0);
        }
        return sb.append(")").toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((target == null) ? 0 : Arrays.hashCode(target));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ProcessingInstructionType other = (ProcessingInstructionType) obj;
        if (target == null) {
            if (other.target != null)
                return false;
        } else if (!Arrays.equals(target, other.target))
            return false;
        return true;
    }
}
