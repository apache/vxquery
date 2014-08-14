package org.apache.vxquery.xtest.util.tests;

import java.io.File;

import org.xml.sax.XMLReader;

public interface IDiskTest {
    public void setBufferSize(int bufferSize);

    public void setFile(File file);

    public void setParser(XMLReader parser);

    public void run();

    public void start();
}
