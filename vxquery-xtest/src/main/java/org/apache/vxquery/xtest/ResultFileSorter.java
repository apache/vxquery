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
package org.apache.vxquery.xtest;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Class to sort the final test results.
 */
public class ResultFileSorter {

    private PrintWriter writer;
    private File resultFile;

    public ResultFileSorter(String path) throws FileNotFoundException {
        this.resultFile = new File(path);
    }

    /**
     * The method to sort the test case results.
     */
    public void sortFile() {
        try {
            FileReader fileReader = new FileReader(this.resultFile);
            BufferedReader reader = new BufferedReader(fileReader);
            String line;
            ArrayList<String> fullText = new ArrayList<>();
            while ((line = reader.readLine()) != null) {
                fullText.add(line);
            }
            System.out.println("Sorting.....");
            Collections.sort(fullText);
            String[] sortedText = fullText.toArray(new String[fullText.size()]);
            this.eraseFile();
            this.writeToFile(sortedText);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Method to erace the current content of the file.
     *
     * @throws FileNotFoundException
     */
    private void eraseFile() throws FileNotFoundException {
        writer = new PrintWriter(resultFile);
        writer.write("");
        writer.close();
        System.out.println("File erased.");
    }

    /**
     * Method to write the sorted content to the file.
     *
     * @param text : The sorted array of test case results.
     * @throws FileNotFoundException
     */
    private void writeToFile(String[] text) throws FileNotFoundException {
        writer = new PrintWriter(resultFile);
        System.out.println("Writing to file started.");
        for (String s : text) {
            writer.write(s + "\n");
        }
        writer.close();
        System.out.println("File writing finished.");
    }
}
