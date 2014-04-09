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

import javax.xml.namespace.QName;

import org.apache.vxquery.xmlquery.query.XQueryConstants;

public enum ErrorCode {
    XPST0001(new QName(XQueryConstants.ERR_NSURI, "XPST0001"), ErrorMessages.ERR_XPST0001_DESCRIPTION),
    XPDY0002(new QName(XQueryConstants.ERR_NSURI, "XPDY0002"), ErrorMessages.ERR_XPDY0002_DESCRIPTION),
    XPST0003(new QName(XQueryConstants.ERR_NSURI, "XPST0003"), ErrorMessages.ERR_XPST0003_DESCRIPTION),
    XPTY0004(new QName(XQueryConstants.ERR_NSURI, "XPTY0004"), ErrorMessages.ERR_XPTY0004_DESCRIPTION),
    XPST0005(new QName(XQueryConstants.ERR_NSURI, "XPST0005"), ErrorMessages.ERR_XPST0005_DESCRIPTION),
    XPTY0006(new QName(XQueryConstants.ERR_NSURI, "XPTY0006"), ErrorMessages.ERR_XPTY0006_DESCRIPTION),
    XPTY0007(new QName(XQueryConstants.ERR_NSURI, "XPTY0007"), ErrorMessages.ERR_XPTY0007_DESCRIPTION),
    XPST0008(new QName(XQueryConstants.ERR_NSURI, "XPST0008"), ErrorMessages.ERR_XPST0008_DESCRIPTION),
    XQST0009(new QName(XQueryConstants.ERR_NSURI, "XQST0009"), ErrorMessages.ERR_XQST0009_DESCRIPTION),
    XPST0010(new QName(XQueryConstants.ERR_NSURI, "XPST0010"), ErrorMessages.ERR_XPST0010_DESCRIPTION),
    XQST0012(new QName(XQueryConstants.ERR_NSURI, "XQST0012"), ErrorMessages.ERR_XQST0012_DESCRIPTION),
    XQST0013(new QName(XQueryConstants.ERR_NSURI, "XQST0013"), ErrorMessages.ERR_XQST0013_DESCRIPTION),
    XQST0014(new QName(XQueryConstants.ERR_NSURI, "XQST0014"), ErrorMessages.ERR_XQST0014_DESCRIPTION),
    XQST0015(new QName(XQueryConstants.ERR_NSURI, "XQST0015"), ErrorMessages.ERR_XQST0015_DESCRIPTION),
    XQST0016(new QName(XQueryConstants.ERR_NSURI, "XQST0016"), ErrorMessages.ERR_XQST0016_DESCRIPTION),
    XPST0017(new QName(XQueryConstants.ERR_NSURI, "XPST0017"), ErrorMessages.ERR_XPST0017_DESCRIPTION),
    XPTY0018(new QName(XQueryConstants.ERR_NSURI, "XPTY0018"), ErrorMessages.ERR_XPTY0018_DESCRIPTION),
    XPTY0019(new QName(XQueryConstants.ERR_NSURI, "XPTY0019"), ErrorMessages.ERR_XPTY0019_DESCRIPTION),
    XPTY0020(new QName(XQueryConstants.ERR_NSURI, "XPTY0020"), ErrorMessages.ERR_XPTY0020_DESCRIPTION),
    XPDY0021(new QName(XQueryConstants.ERR_NSURI, "XPDY0021"), ErrorMessages.ERR_XPDY0021_DESCRIPTION),
    XQST0022(new QName(XQueryConstants.ERR_NSURI, "XQST0022"), ErrorMessages.ERR_XQST0022_DESCRIPTION),
    XQTY0023(new QName(XQueryConstants.ERR_NSURI, "XQTY0023"), ErrorMessages.ERR_XQTY0023_DESCRIPTION),
    XQTY0024(new QName(XQueryConstants.ERR_NSURI, "XQTY0024"), ErrorMessages.ERR_XQTY0024_DESCRIPTION),
    XQDY0025(new QName(XQueryConstants.ERR_NSURI, "XQDY0025"), ErrorMessages.ERR_XQDY0025_DESCRIPTION),
    XQDY0026(new QName(XQueryConstants.ERR_NSURI, "XQDY0026"), ErrorMessages.ERR_XQDY0026_DESCRIPTION),
    XQDY0027(new QName(XQueryConstants.ERR_NSURI, "XQDY0027"), ErrorMessages.ERR_XQDY0027_DESCRIPTION),
    XQTY0028(new QName(XQueryConstants.ERR_NSURI, "XQTY0028"), ErrorMessages.ERR_XQTY0028_DESCRIPTION),
    XQDY0029(new QName(XQueryConstants.ERR_NSURI, "XQDY0029"), ErrorMessages.ERR_XQDY0029_DESCRIPTION),
    XQTY0030(new QName(XQueryConstants.ERR_NSURI, "XQTY0030"), ErrorMessages.ERR_XQTY0030_DESCRIPTION),
    XQST0031(new QName(XQueryConstants.ERR_NSURI, "XQST0031"), ErrorMessages.ERR_XQST0031_DESCRIPTION),
    XQST0032(new QName(XQueryConstants.ERR_NSURI, "XQST0032"), ErrorMessages.ERR_XQST0032_DESCRIPTION),
    XQST0033(new QName(XQueryConstants.ERR_NSURI, "XQST0033"), ErrorMessages.ERR_XQST0033_DESCRIPTION),
    XQST0034(new QName(XQueryConstants.ERR_NSURI, "XQST0034"), ErrorMessages.ERR_XQST0034_DESCRIPTION),
    XQST0035(new QName(XQueryConstants.ERR_NSURI, "XQST0035"), ErrorMessages.ERR_XQST0035_DESCRIPTION),
    XQST0036(new QName(XQueryConstants.ERR_NSURI, "XQST0036"), ErrorMessages.ERR_XQST0036_DESCRIPTION),
    XQST0037(new QName(XQueryConstants.ERR_NSURI, "XQST0037"), ErrorMessages.ERR_XQST0037_DESCRIPTION),
    XQST0038(new QName(XQueryConstants.ERR_NSURI, "XQST0038"), ErrorMessages.ERR_XQST0038_DESCRIPTION),
    XQST0039(new QName(XQueryConstants.ERR_NSURI, "XQST0039"), ErrorMessages.ERR_XQST0039_DESCRIPTION),
    XQST0040(new QName(XQueryConstants.ERR_NSURI, "XQST0040"), ErrorMessages.ERR_XQST0040_DESCRIPTION),
    XQDY0041(new QName(XQueryConstants.ERR_NSURI, "XQDY0041"), ErrorMessages.ERR_XQDY0041_DESCRIPTION),
    XQST0042(new QName(XQueryConstants.ERR_NSURI, "XQST0042"), ErrorMessages.ERR_XQST0042_DESCRIPTION),
    XQST0043(new QName(XQueryConstants.ERR_NSURI, "XQST0043"), ErrorMessages.ERR_XQST0043_DESCRIPTION),
    XQDY0044(new QName(XQueryConstants.ERR_NSURI, "XQDY0044"), ErrorMessages.ERR_XQDY0044_DESCRIPTION),
    XQST0045(new QName(XQueryConstants.ERR_NSURI, "XQST0045"), ErrorMessages.ERR_XQST0045_DESCRIPTION),
    XQST0046(new QName(XQueryConstants.ERR_NSURI, "XQST0046"), ErrorMessages.ERR_XQST0046_DESCRIPTION),
    XQST0047(new QName(XQueryConstants.ERR_NSURI, "XQST0047"), ErrorMessages.ERR_XQST0047_DESCRIPTION),
    XQST0048(new QName(XQueryConstants.ERR_NSURI, "XQST0048"), ErrorMessages.ERR_XQST0048_DESCRIPTION),
    XQST0049(new QName(XQueryConstants.ERR_NSURI, "XQST0049"), ErrorMessages.ERR_XQST0049_DESCRIPTION),
    XPDY0050(new QName(XQueryConstants.ERR_NSURI, "XPDY0050"), ErrorMessages.ERR_XPDY0050_DESCRIPTION),
    XPST0051(new QName(XQueryConstants.ERR_NSURI, "XPST0051"), ErrorMessages.ERR_XPST0051_DESCRIPTION),
    XQDY0052(new QName(XQueryConstants.ERR_NSURI, "XQDY0052"), ErrorMessages.ERR_XQDY0052_DESCRIPTION),
    XQST0053(new QName(XQueryConstants.ERR_NSURI, "XQST0053"), ErrorMessages.ERR_XQST0053_DESCRIPTION),
    XQST0054(new QName(XQueryConstants.ERR_NSURI, "XQST0054"), ErrorMessages.ERR_XQST0054_DESCRIPTION),
    XQST0055(new QName(XQueryConstants.ERR_NSURI, "XQST0055"), ErrorMessages.ERR_XQST0055_DESCRIPTION),
    XQST0056(new QName(XQueryConstants.ERR_NSURI, "XQST0056"), ErrorMessages.ERR_XQST0056_DESCRIPTION),
    XQST0057(new QName(XQueryConstants.ERR_NSURI, "XQST0057"), ErrorMessages.ERR_XQST0057_DESCRIPTION),
    XQST0058(new QName(XQueryConstants.ERR_NSURI, "XQST0058"), ErrorMessages.ERR_XQST0058_DESCRIPTION),
    XQST0059(new QName(XQueryConstants.ERR_NSURI, "XQST0059"), ErrorMessages.ERR_XQST0059_DESCRIPTION),
    XQST0060(new QName(XQueryConstants.ERR_NSURI, "XQST0060"), ErrorMessages.ERR_XQST0060_DESCRIPTION),
    XQDY0061(new QName(XQueryConstants.ERR_NSURI, "XQDY0061"), ErrorMessages.ERR_XQDY0061_DESCRIPTION),
    XQDY0062(new QName(XQueryConstants.ERR_NSURI, "XQDY0062"), ErrorMessages.ERR_XQDY0062_DESCRIPTION),
    XQST0063(new QName(XQueryConstants.ERR_NSURI, "XQST0063"), ErrorMessages.ERR_XQST0063_DESCRIPTION),
    XQDY0064(new QName(XQueryConstants.ERR_NSURI, "XQDY0064"), ErrorMessages.ERR_XQDY0064_DESCRIPTION),
    XQST0065(new QName(XQueryConstants.ERR_NSURI, "XQST0065"), ErrorMessages.ERR_XQST0065_DESCRIPTION),
    XQST0066(new QName(XQueryConstants.ERR_NSURI, "XQST0066"), ErrorMessages.ERR_XQST0066_DESCRIPTION),
    XQST0067(new QName(XQueryConstants.ERR_NSURI, "XQST0067"), ErrorMessages.ERR_XQST0067_DESCRIPTION),
    XQST0068(new QName(XQueryConstants.ERR_NSURI, "XQST0068"), ErrorMessages.ERR_XQST0068_DESCRIPTION),
    XQST0069(new QName(XQueryConstants.ERR_NSURI, "XQST0069"), ErrorMessages.ERR_XQST0069_DESCRIPTION),
    XQST0070(new QName(XQueryConstants.ERR_NSURI, "XQST0070"), ErrorMessages.ERR_XQST0070_DESCRIPTION),
    XQST0071(new QName(XQueryConstants.ERR_NSURI, "XQST0071"), ErrorMessages.ERR_XQST0071_DESCRIPTION),
    XQDY0072(new QName(XQueryConstants.ERR_NSURI, "XQDY0072"), ErrorMessages.ERR_XQDY0072_DESCRIPTION),
    XQST0073(new QName(XQueryConstants.ERR_NSURI, "XQST0073"), ErrorMessages.ERR_XQST0073_DESCRIPTION),
    XQDY0074(new QName(XQueryConstants.ERR_NSURI, "XQDY0074"), ErrorMessages.ERR_XQDY0074_DESCRIPTION),
    XQST0075(new QName(XQueryConstants.ERR_NSURI, "XQST0075"), ErrorMessages.ERR_XQST0075_DESCRIPTION),
    XQST0076(new QName(XQueryConstants.ERR_NSURI, "XQST0076"), ErrorMessages.ERR_XQST0076_DESCRIPTION),
    XQST0077(new QName(XQueryConstants.ERR_NSURI, "XQST0077"), ErrorMessages.ERR_XQST0077_DESCRIPTION),
    XQST0078(new QName(XQueryConstants.ERR_NSURI, "XQST0078"), ErrorMessages.ERR_XQST0078_DESCRIPTION),
    XQST0079(new QName(XQueryConstants.ERR_NSURI, "XQST0079"), ErrorMessages.ERR_XQST0079_DESCRIPTION),
    XPST0080(new QName(XQueryConstants.ERR_NSURI, "XPST0080"), ErrorMessages.ERR_XPST0080_DESCRIPTION),
    XPST0081(new QName(XQueryConstants.ERR_NSURI, "XPST0081"), ErrorMessages.ERR_XPST0081_DESCRIPTION),
    XQST0082(new QName(XQueryConstants.ERR_NSURI, "XQST0082"), ErrorMessages.ERR_XQST0082_DESCRIPTION),
    XPST0083(new QName(XQueryConstants.ERR_NSURI, "XPST0083"), ErrorMessages.ERR_XPST0083_DESCRIPTION),
    XQDY0084(new QName(XQueryConstants.ERR_NSURI, "XQDY0084"), ErrorMessages.ERR_XQDY0084_DESCRIPTION),
    XQST0085(new QName(XQueryConstants.ERR_NSURI, "XQST0085"), ErrorMessages.ERR_XQST0085_DESCRIPTION),
    XQTY0086(new QName(XQueryConstants.ERR_NSURI, "XQTY0086"), ErrorMessages.ERR_XQTY0086_DESCRIPTION),
    XQST0087(new QName(XQueryConstants.ERR_NSURI, "XQST0087"), ErrorMessages.ERR_XQST0087_DESCRIPTION),
    XQST0088(new QName(XQueryConstants.ERR_NSURI, "XQST0088"), ErrorMessages.ERR_XQST0088_DESCRIPTION),
    XQST0089(new QName(XQueryConstants.ERR_NSURI, "XQST0089"), ErrorMessages.ERR_XQST0089_DESCRIPTION),
    XQST0090(new QName(XQueryConstants.ERR_NSURI, "XQST0090"), ErrorMessages.ERR_XQST0090_DESCRIPTION),
    XQDY0091(new QName(XQueryConstants.ERR_NSURI, "XQDY0091"), ErrorMessages.ERR_XQDY0091_DESCRIPTION),
    XQDY0092(new QName(XQueryConstants.ERR_NSURI, "XQDY0092"), ErrorMessages.ERR_XQDY0092_DESCRIPTION),
    XQST0093(new QName(XQueryConstants.ERR_NSURI, "XQST0093"), ErrorMessages.ERR_XQST0093_DESCRIPTION),
    FOER0000(new QName(XQueryConstants.ERR_NSURI, "FOER0000"), ErrorMessages.ERR_FOER0000_DESCRIPTION),
    FOAR0001(new QName(XQueryConstants.ERR_NSURI, "FOAR0001"), ErrorMessages.ERR_FOAR0001_DESCRIPTION),
    FOAR0002(new QName(XQueryConstants.ERR_NSURI, "FOAR0002"), ErrorMessages.ERR_FOAR0002_DESCRIPTION),
    FOCA0001(new QName(XQueryConstants.ERR_NSURI, "FOCA0001"), ErrorMessages.ERR_FOCA0001_DESCRIPTION),
    FOCA0002(new QName(XQueryConstants.ERR_NSURI, "FOCA0002"), ErrorMessages.ERR_FOCA0002_DESCRIPTION),
    FOCA0003(new QName(XQueryConstants.ERR_NSURI, "FOCA0003"), ErrorMessages.ERR_FOCA0003_DESCRIPTION),
    FOCA0005(new QName(XQueryConstants.ERR_NSURI, "FOCA0005"), ErrorMessages.ERR_FOCA0005_DESCRIPTION),
    FOCA0006(new QName(XQueryConstants.ERR_NSURI, "FOCA0006"), ErrorMessages.ERR_FOCA0006_DESCRIPTION),
    FOCH0001(new QName(XQueryConstants.ERR_NSURI, "FOCH0001"), ErrorMessages.ERR_FOCH0001_DESCRIPTION),
    FOCH0002(new QName(XQueryConstants.ERR_NSURI, "FOCH0002"), ErrorMessages.ERR_FOCH0002_DESCRIPTION),
    FOCH0003(new QName(XQueryConstants.ERR_NSURI, "FOCH0003"), ErrorMessages.ERR_FOCH0003_DESCRIPTION),
    FOCH0004(new QName(XQueryConstants.ERR_NSURI, "FOCH0004"), ErrorMessages.ERR_FOCH0004_DESCRIPTION),
    FODC0001(new QName(XQueryConstants.ERR_NSURI, "FODC0001"), ErrorMessages.ERR_FODC0001_DESCRIPTION),
    FODC0002(new QName(XQueryConstants.ERR_NSURI, "FODC0002"), ErrorMessages.ERR_FODC0002_DESCRIPTION),
    FODC0003(new QName(XQueryConstants.ERR_NSURI, "FODC0003"), ErrorMessages.ERR_FODC0003_DESCRIPTION),
    FODC0004(new QName(XQueryConstants.ERR_NSURI, "FODC0004"), ErrorMessages.ERR_FODC0004_DESCRIPTION),
    FODC0005(new QName(XQueryConstants.ERR_NSURI, "FODC0005"), ErrorMessages.ERR_FODC0005_DESCRIPTION),
    FODT0001(new QName(XQueryConstants.ERR_NSURI, "FODT0001"), ErrorMessages.ERR_FODT0001_DESCRIPTION),
    FODT0002(new QName(XQueryConstants.ERR_NSURI, "FODT0002"), ErrorMessages.ERR_FODT0002_DESCRIPTION),
    FODT0003(new QName(XQueryConstants.ERR_NSURI, "FODT0003"), ErrorMessages.ERR_FODT0003_DESCRIPTION),
    FONS0004(new QName(XQueryConstants.ERR_NSURI, "FONS0004"), ErrorMessages.ERR_FONS0004_DESCRIPTION),
    FONS0005(new QName(XQueryConstants.ERR_NSURI, "FONS0005"), ErrorMessages.ERR_FONS0005_DESCRIPTION),
    FORG0001(new QName(XQueryConstants.ERR_NSURI, "FORG0001"), ErrorMessages.ERR_FORG0001_DESCRIPTION),
    FORG0002(new QName(XQueryConstants.ERR_NSURI, "FORG0002"), ErrorMessages.ERR_FORG0002_DESCRIPTION),
    FORG0003(new QName(XQueryConstants.ERR_NSURI, "FORG0003"), ErrorMessages.ERR_FORG0003_DESCRIPTION),
    FORG0004(new QName(XQueryConstants.ERR_NSURI, "FORG0004"), ErrorMessages.ERR_FORG0004_DESCRIPTION),
    FORG0005(new QName(XQueryConstants.ERR_NSURI, "FORG0005"), ErrorMessages.ERR_FORG0005_DESCRIPTION),
    FORG0006(new QName(XQueryConstants.ERR_NSURI, "FORG0006"), ErrorMessages.ERR_FORG0006_DESCRIPTION),
    FORG0008(new QName(XQueryConstants.ERR_NSURI, "FORG0008"), ErrorMessages.ERR_FORG0008_DESCRIPTION),
    FORG0009(new QName(XQueryConstants.ERR_NSURI, "FORG0009"), ErrorMessages.ERR_FORG0009_DESCRIPTION),
    FORX0001(new QName(XQueryConstants.ERR_NSURI, "FORX0001"), ErrorMessages.ERR_FORX0001_DESCRIPTION),
    FORX0002(new QName(XQueryConstants.ERR_NSURI, "FORX0002"), ErrorMessages.ERR_FORX0002_DESCRIPTION),
    FORX0003(new QName(XQueryConstants.ERR_NSURI, "FORX0003"), ErrorMessages.ERR_FORX0003_DESCRIPTION),
    FORX0004(new QName(XQueryConstants.ERR_NSURI, "FORX0004"), ErrorMessages.ERR_FORX0004_DESCRIPTION),
    FOTY0012(new QName(XQueryConstants.ERR_NSURI, "FOTY0012"), ErrorMessages.ERR_FOTY0012_DESCRIPTION),

    SYSE0001(new QName(XQueryConstants.ERR_NSURI, "SYSE0001"), ErrorMessages.ERR_SYSE0001_DESCRIPTION),
    TODO(new QName(XQueryConstants.ERR_NSURI, "TODO"), ErrorMessages.ERR_TODO_DESCRIPTION),
    ;
    
    private QName qname;
    private String description;

    private ErrorCode(QName qname, String description) {
        this.qname = qname;
        this.description = description;
    }

    public QName getQname() {
        return qname;
    }

    public String getDescription() {
        return description;
    }
}
