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

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Properties;

public class ErrorMessages {
    private static final String[] MESSAGE_BUNDLES = { "/org/apache/vxquery/xmlquery/exceptions/XMLQueryErrorMessages.properties" };

    static {
        for (String bundle : MESSAGE_BUNDLES) {
            Properties props = new Properties();
            try {
                props.load(ErrorMessages.class.getResourceAsStream(bundle));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            Class<ErrorMessages> klass = ErrorMessages.class;
            for (Object key : props.keySet()) {
                Field f;
                try {
                    f = klass.getField((String) key);
                    f.set(null, props.getProperty((String) key));
                } catch (SecurityException e) {
                    throw new RuntimeException(e);
                } catch (NoSuchFieldException e) {
                    throw new RuntimeException(e);
                } catch (IllegalArgumentException e) {
                    throw new RuntimeException(e);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private ErrorMessages() {
    }

    public static String ERR_XPST0001_DESCRIPTION;
    public static String ERR_XPDY0002_DESCRIPTION;
    public static String ERR_XPST0003_DESCRIPTION;
    public static String ERR_XPTY0004_DESCRIPTION;
    public static String ERR_XPST0005_DESCRIPTION;
    public static String ERR_XPTY0006_DESCRIPTION;
    public static String ERR_XPTY0007_DESCRIPTION;
    public static String ERR_XPST0008_DESCRIPTION;
    public static String ERR_XQST0009_DESCRIPTION;
    public static String ERR_XPST0010_DESCRIPTION;
    public static String ERR_XQST0012_DESCRIPTION;
    public static String ERR_XQST0013_DESCRIPTION;
    public static String ERR_XQST0014_DESCRIPTION;
    public static String ERR_XQST0015_DESCRIPTION;
    public static String ERR_XQST0016_DESCRIPTION;
    public static String ERR_XPST0017_DESCRIPTION;
    public static String ERR_XPTY0018_DESCRIPTION;
    public static String ERR_XPTY0019_DESCRIPTION;
    public static String ERR_XPTY0020_DESCRIPTION;
    public static String ERR_XPDY0021_DESCRIPTION;
    public static String ERR_XQST0022_DESCRIPTION;
    public static String ERR_XQTY0023_DESCRIPTION;
    public static String ERR_XQTY0024_DESCRIPTION;
    public static String ERR_XQDY0025_DESCRIPTION;
    public static String ERR_XQDY0026_DESCRIPTION;
    public static String ERR_XQDY0027_DESCRIPTION;
    public static String ERR_XQTY0028_DESCRIPTION;
    public static String ERR_XQDY0029_DESCRIPTION;
    public static String ERR_XQTY0030_DESCRIPTION;
    public static String ERR_XQST0031_DESCRIPTION;
    public static String ERR_XQST0032_DESCRIPTION;
    public static String ERR_XQST0033_DESCRIPTION;
    public static String ERR_XQST0034_DESCRIPTION;
    public static String ERR_XQST0035_DESCRIPTION;
    public static String ERR_XQST0036_DESCRIPTION;
    public static String ERR_XQST0037_DESCRIPTION;
    public static String ERR_XQST0038_DESCRIPTION;
    public static String ERR_XQST0039_DESCRIPTION;
    public static String ERR_XQST0040_DESCRIPTION;
    public static String ERR_XQDY0041_DESCRIPTION;
    public static String ERR_XQST0042_DESCRIPTION;
    public static String ERR_XQST0043_DESCRIPTION;
    public static String ERR_XQDY0044_DESCRIPTION;
    public static String ERR_XQST0045_DESCRIPTION;
    public static String ERR_XQST0046_DESCRIPTION;
    public static String ERR_XQST0047_DESCRIPTION;
    public static String ERR_XQST0048_DESCRIPTION;
    public static String ERR_XQST0049_DESCRIPTION;
    public static String ERR_XPDY0050_DESCRIPTION;
    public static String ERR_XPST0051_DESCRIPTION;
    public static String ERR_XQDY0052_DESCRIPTION;
    public static String ERR_XQST0053_DESCRIPTION;
    public static String ERR_XQST0054_DESCRIPTION;
    public static String ERR_XQST0055_DESCRIPTION;
    public static String ERR_XQST0056_DESCRIPTION;
    public static String ERR_XQST0057_DESCRIPTION;
    public static String ERR_XQST0058_DESCRIPTION;
    public static String ERR_XQST0059_DESCRIPTION;
    public static String ERR_XQST0060_DESCRIPTION;
    public static String ERR_XQDY0061_DESCRIPTION;
    public static String ERR_XQDY0062_DESCRIPTION;
    public static String ERR_XQST0063_DESCRIPTION;
    public static String ERR_XQDY0064_DESCRIPTION;
    public static String ERR_XQST0065_DESCRIPTION;
    public static String ERR_XQST0066_DESCRIPTION;
    public static String ERR_XQST0067_DESCRIPTION;
    public static String ERR_XQST0068_DESCRIPTION;
    public static String ERR_XQST0069_DESCRIPTION;
    public static String ERR_XQST0070_DESCRIPTION;
    public static String ERR_XQST0071_DESCRIPTION;
    public static String ERR_XQDY0072_DESCRIPTION;
    public static String ERR_XQST0073_DESCRIPTION;
    public static String ERR_XQDY0074_DESCRIPTION;
    public static String ERR_XQST0075_DESCRIPTION;
    public static String ERR_XQST0076_DESCRIPTION;
    public static String ERR_XQST0077_DESCRIPTION;
    public static String ERR_XQST0078_DESCRIPTION;
    public static String ERR_XQST0079_DESCRIPTION;
    public static String ERR_XPST0080_DESCRIPTION;
    public static String ERR_XPST0081_DESCRIPTION;
    public static String ERR_XQST0082_DESCRIPTION;
    public static String ERR_XPST0083_DESCRIPTION;
    public static String ERR_XQDY0084_DESCRIPTION;
    public static String ERR_XQST0085_DESCRIPTION;
    public static String ERR_XQTY0086_DESCRIPTION;
    public static String ERR_XQST0087_DESCRIPTION;
    public static String ERR_XQST0088_DESCRIPTION;
    public static String ERR_XQST0089_DESCRIPTION;
    public static String ERR_XQST0090_DESCRIPTION;
    public static String ERR_XQDY0091_DESCRIPTION;
    public static String ERR_XQDY0092_DESCRIPTION;
    public static String ERR_XQST0093_DESCRIPTION;
    public static String ERR_FOER0000_DESCRIPTION;
    public static String ERR_FOAR0001_DESCRIPTION;
    public static String ERR_FOAR0002_DESCRIPTION;
    public static String ERR_FOCA0001_DESCRIPTION;
    public static String ERR_FOCA0002_DESCRIPTION;
    public static String ERR_FOCA0003_DESCRIPTION;
    public static String ERR_FOCA0005_DESCRIPTION;
    public static String ERR_FOCA0006_DESCRIPTION;
    public static String ERR_FOCH0001_DESCRIPTION;
    public static String ERR_FOCH0002_DESCRIPTION;
    public static String ERR_FOCH0003_DESCRIPTION;
    public static String ERR_FOCH0004_DESCRIPTION;
    public static String ERR_FODC0001_DESCRIPTION;
    public static String ERR_FODC0002_DESCRIPTION;
    public static String ERR_FODC0003_DESCRIPTION;
    public static String ERR_FODC0004_DESCRIPTION;
    public static String ERR_FODC0005_DESCRIPTION;
    public static String ERR_FODT0001_DESCRIPTION;
    public static String ERR_FODT0002_DESCRIPTION;
    public static String ERR_FODT0003_DESCRIPTION;
    public static String ERR_FONS0004_DESCRIPTION;
    public static String ERR_FONS0005_DESCRIPTION;
    public static String ERR_FORG0001_DESCRIPTION;
    public static String ERR_FORG0002_DESCRIPTION;
    public static String ERR_FORG0003_DESCRIPTION;
    public static String ERR_FORG0004_DESCRIPTION;
    public static String ERR_FORG0005_DESCRIPTION;
    public static String ERR_FORG0006_DESCRIPTION;
    public static String ERR_FORG0008_DESCRIPTION;
    public static String ERR_FORG0009_DESCRIPTION;
    public static String ERR_FORX0001_DESCRIPTION;
    public static String ERR_FORX0002_DESCRIPTION;
    public static String ERR_FORX0003_DESCRIPTION;
    public static String ERR_FORX0004_DESCRIPTION;
    public static String ERR_FOTY0012_DESCRIPTION;

    public static String ERR_JNDY0003_DESCRIPTION;
    public static String ERR_JNTY0004_DESCRIPTION;
    public static String ERR_JNUP0005_DESCRIPTION;
    public static String ERR_JNUP0006_DESCRIPTION;
    public static String ERR_JNUP0007_DESCRIPTION;
    public static String ERR_JNUP0008_DESCRIPTION;
    public static String ERR_JNUP0009_DESCRIPTION;
    public static String ERR_JNUP0010_DESCRIPTION;
    public static String ERR_JNTY0011_DESCRIPTION;
    public static String ERR_JNSE0012_DESCRIPTION;
    public static String ERR_JNSE0014_DESCRIPTION;
    public static String ERR_JNSE0015_DESCRIPTION;
    public static String ERR_JNUP0016_DESCRIPTION;
    public static String ERR_JNTY0018_DESCRIPTION;
    public static String ERR_JNUP0019_DESCRIPTION;
    public static String ERR_JNTY0020_DESCRIPTION;
    public static String ERR_JNDY0021_DESCRIPTION;
    public static String ERR_JNSE0022_DESCRIPTION;
    public static String ERR_JNTY0023_DESCRIPTION;
    public static String ERR_JNTY0024_DESCRIPTION;

    public static String ERR_SYSE0001_DESCRIPTION;
    public static String ERR_TODO_DESCRIPTION;
}
