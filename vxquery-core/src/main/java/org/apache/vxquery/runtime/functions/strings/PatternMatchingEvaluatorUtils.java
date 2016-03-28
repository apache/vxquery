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
package org.apache.vxquery.runtime.functions.strings;

import java.util.regex.Pattern;

import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.datamodel.values.XDMConstants;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.util.FunctionHelper;

public class PatternMatchingEvaluatorUtils {
    public static int toFlag(String pattern) throws SystemException {
        int flag = 0;
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            switch (c) {
                case 's':
                    flag |= Pattern.DOTALL;
                    break;
                case 'm':
                    flag |= Pattern.MULTILINE;
                    break;
                case 'i':
                    flag |= Pattern.CASE_INSENSITIVE;
                    break;
                case 'x':
                    flag |= Pattern.COMMENTS;
                    break;
                case 'q':
                    break;
                default:
                    throw new SystemException(ErrorCode.FORX0001);
            }
        }
        flag |= Pattern.UNICODE_CHARACTER_CLASS;
        return flag;
    }

    public static void checkInput(TaggedValuePointable tvp, TaggedValuePointable tvp1, SequencePointable seqp,
            UTF8StringPointable stringp1) throws SystemException {
        if (tvp1.getTag() == ValueTag.SEQUENCE_TAG) {
            tvp1.getValue(seqp);
            if (seqp.getEntryCount() == 0) {
                XDMConstants.setEmptyString(tvp);
                tvp.getValue(stringp1);
            } else {
                throw new SystemException(ErrorCode.FORG0006);
            }
        } else {
            if (!FunctionHelper.isDerivedFromString(tvp1.getTag())) {
                throw new SystemException(ErrorCode.FORG0006);
            }
            tvp1.getValue(stringp1);
        }
    }
}
