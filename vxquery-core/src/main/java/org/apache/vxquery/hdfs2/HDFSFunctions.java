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
package org.apache.vxquery.hdfs2;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hyracks.api.client.NodeControllerInfo;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.hdfs.ContextFactory;
import org.apache.hyracks.hdfs2.dataflow.FileSplitsFactory;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class HDFSFunctions {

    private Configuration conf;
    private FileSystem fs;
    private String confPath;
    private Job job;
    private InputFormat inputFormat;
    private List<InputSplit> splits;
    private ArrayList<ArrayList<String>> nodes;
    private HashMap<Integer, String> schedule;
    private static final String TEMP = "java.io.tmpdir";
    private static final String DFS_PATH = "vxquery_splits_schedule.txt";
    private static final String FILEPATH = System.getProperty(TEMP) + "splits_schedule.txt";
    protected static final Logger LOGGER = Logger.getLogger(HDFSFunctions.class.getName());
    private final Map<String, NodeControllerInfo> nodeControllerInfos;

    /**
     * Create the configuration and add the paths for core-site and hdfs-site as resources.
     * Initialize an instance of HDFS FileSystem for this configuration.
     *
     * @param nodeControllerInfos
     *            Map of the node to its attributes
     * @param hdfsConf
     *            Hdfs path to config
     */
    public HDFSFunctions(Map<String, NodeControllerInfo> nodeControllerInfos, String hdfsConf) {
        this.conf = new Configuration();
        this.nodeControllerInfos = nodeControllerInfos;
        this.confPath = hdfsConf;
    }

    /**
     * Create the needed objects for reading the splits of the filepath given as argument.
     * This method should run before the scheduleSplits method.
     *
     * @param filepath
     *            Path to config.
     * @param tag
     *            Tag to read.
     */
    @SuppressWarnings({ "deprecation", "unchecked" })
    public void setJob(String filepath, String tag) {
        try {
            conf.set("start_tag", "<" + tag + ">");
            conf.set("end_tag", "</" + tag + ">");
            job = new Job(conf, "Read from HDFS");
            Path input = new Path(filepath);
            FileInputFormat.addInputPath(job, input);
            job.setInputFormatClass(XmlCollectionWithTagInputFormat.class);
            inputFormat = ReflectionUtils.newInstance(job.getInputFormatClass(), job.getConfiguration());
            splits = inputFormat.getSplits(job);
        } catch (IOException | ClassNotFoundException | InterruptedException e) {
            if (LOGGER.isLoggable(Level.SEVERE)) {
                LOGGER.severe(e.getMessage());
            }
        }
    }

    /**
     * Returns true if the file path exists or it is located somewhere in the home directory of the user that called the function.
     * Searches in subdirectories of the home directory too.
     *
     * @param filename
     *            HDFS file path.
     * @return boolean
     *         True if located in HDFS.
     * @throws IOException
     *         If searching for the filepath throws {@link IOException}
     */
    public boolean isLocatedInHDFS(String filename) throws IllegalArgumentException, IOException {
        //search file path
        if (fs.exists(new Path(filename))) {
            return true;
        }
        return searchInDirectory(fs.getHomeDirectory(), filename) != null;
    }

    /**
     * Searches the given directory for the file.
     *
     * @param directory
     *            to search
     * @param filename
     *            of file we want
     * @return path if file exists in this directory.else return null.
     */
    public Path searchInDirectory(Path directory, String filename) {
        //Search the files and folder in this Path to find the one matching the filename.
        try {
            RemoteIterator<LocatedFileStatus> it = fs.listFiles(directory, true);
            String[] parts;
            Path path;
            while (it.hasNext()) {
                path = it.next().getPath();
                parts = path.toString().split("/");
                if (parts[parts.length - 1].equals(filename)) {
                    return path;
                }
            }
        } catch (IOException e) {
            if (LOGGER.isLoggable(Level.SEVERE)) {
                LOGGER.severe(e.getMessage());
            }
        }
        return null;
    }

    /**
     * Read the cluster properties file and locate the HDFS_CONF variable that is the directory path for the
     * hdfs configuration if the system environment variable HDFS_CONF is not set.
     *
     * @return true if is successfully finds the Hadoop/HDFS home directory
     */
    private boolean locateConf() {
        if (this.confPath == null) {
            //As a last resort, try getting the configuration from the system environment
            //Some systems won't have this set.
            this.confPath = System.getenv("HADOOP_CONF_DIR");
        }
        return this.confPath != null;
    }

    /**
     * Upload a file/directory to HDFS.Filepath is the path in the local file system.dir is the destination path.
     *
     * @param filepath
     *            file to upload
     * @param dir
     *            HDFS directory to save the file
     * @return boolean
     */
    public boolean put(String filepath, String dir) {
        if (this.fs != null) {
            Path path = new Path(filepath);
            Path dest = new Path(dir);
            try {
                if (fs.exists(dest)) {
                    fs.delete(dest, true); //recursive delete
                }
            } catch (IOException e) {
                if (LOGGER.isLoggable(Level.SEVERE)) {
                    LOGGER.severe(e.getMessage());
                }
            }
            try {
                fs.copyFromLocalFile(path, dest);
            } catch (IOException e) {
                if (LOGGER.isLoggable(Level.SEVERE)) {
                    LOGGER.severe(e.getMessage());
                }
            }
        }
        return false;
    }

    /**
     * Get instance of the HDFSfile system if it is configured correctly.
     * Return null if there is no instance.
     *
     * @return FileSystem
     */
    public FileSystem getFileSystem() {
        if (locateConf()) {
            conf.addResource(new Path(this.confPath + "/core-site.xml"));
            conf.addResource(new Path(this.confPath + "/hdfs-site.xml"));
            try {
                fs = FileSystem.get(conf);
                return this.fs;
            } catch (IOException e) {
                if (LOGGER.isLoggable(Level.SEVERE)) {
                    LOGGER.severe(e.getMessage());
                }
            }
        } else {
            if (LOGGER.isLoggable(Level.SEVERE)) {
                LOGGER.severe("Could not locate HDFS configuration folder.");
            }
        }
        return null;
    }

    public HashMap<String, ArrayList<Integer>> getLocationsOfSplits() throws IOException {
        HashMap<String, ArrayList<Integer>> splitsMap = new HashMap<>();
        ArrayList<Integer> temp;
        int i = 0;
        String hostname;
        for (InputSplit s : this.splits) {
            SplitLocationInfo[] info = s.getLocationInfo();
            hostname = info[0].getLocation();
            if (splitsMap.containsKey(hostname)) {
                temp = splitsMap.get(hostname);
                temp.add(i);
            } else {
                temp = new ArrayList<>();
                temp.add(i);
                splitsMap.put(hostname, temp);
            }
            i++;
        }

        return splitsMap;
    }

    public void scheduleSplits() throws IOException, ParserConfigurationException, SAXException {
        schedule = new HashMap<>();
        ArrayList<String> empty = new ArrayList<>();
        HashMap<String, ArrayList<Integer>> splitsMap = this.getLocationsOfSplits();
        readNodesFromXML();
        int count = this.splits.size();

        String node;
        for (ArrayList<String> info : this.nodes) {
            node = info.get(1);
            if (splitsMap.containsKey(node)) {
                for (Integer split : splitsMap.get(node)) {
                    schedule.put(split, node);
                    count--;
                }
                splitsMap.remove(node);
            } else {
                empty.add(node);
            }
        }

        //Check if every split got assigned to a node
        if (count != 0) {
            ArrayList<Integer> remaining = new ArrayList<>();
            // Find remaining splits
            for (InputSplit s : this.splits) {
                int i = 0;
                if (!schedule.containsKey(i)) {
                    remaining.add(i);
                }
            }

            if (!empty.isEmpty()) {
                int nodeNumber = 0;
                for (int split : remaining) {
                    if (nodeNumber == empty.size()) {
                        nodeNumber = 0;
                    }
                    schedule.put(split, empty.get(nodeNumber));
                    nodeNumber++;
                }
            }
        }
    }

    /**
     * Read the hostname and the ip address of every node from the xml cluster configuration file.
     * Save the information inside nodes.
     */
    public void readNodesFromXML() {
        nodes = new ArrayList<>();
        for (NodeControllerInfo ncInfo : nodeControllerInfos.values()) {
            //Will this include the master node? Is that bad?
            ArrayList<String> info = new ArrayList<>();
            info.add(ncInfo.getNodeId());
            info.add(ncInfo.getNetworkAddress().getAddress());
            nodes.add(info);
        }
    }

    /**
     * Writes the schedule to a temporary file, then uploads the file to the HDFS.
     *
     * @throws UnsupportedEncodingException
     *            The encoding of the file is not correct     
     * @throws FileNotFoundException
     *            The file doesn't exist
     */
    public void addScheduleToDistributedCache() throws FileNotFoundException, UnsupportedEncodingException {
        PrintWriter writer = new PrintWriter(FILEPATH, "UTF-8");
        for (int split : this.schedule.keySet()) {
            writer.write(split + "," + this.schedule.get(split));
        }
        writer.close();
        // Add file to HDFS
        this.put(FILEPATH, DFS_PATH);
    }

    public RecordReader getReader() {

        List<FileSplit> fileSplits = new ArrayList<>();
        for (int i = 0; i < splits.size(); i++) {
            fileSplits.add((FileSplit) splits.get(i));
        }
        FileSplitsFactory splitsFactory;
        try {
            splitsFactory = new FileSplitsFactory(fileSplits);
            List<FileSplit> inputSplits = splitsFactory.getSplits();
            ContextFactory ctxFactory = new ContextFactory();
            int size = inputSplits.size();
            for (int i = 0; i < size; i++) {
                /**
                 * read the split
                 */
                TaskAttemptContext context;
                try {
                    context = ctxFactory.createContext(job.getConfiguration(), i);
                    RecordReader reader = inputFormat.createRecordReader(inputSplits.get(i), context);
                    reader.initialize(inputSplits.get(i), context);
                    return reader;
                } catch (IOException | InterruptedException e) {
                    if (LOGGER.isLoggable(Level.SEVERE)) {
                        LOGGER.severe(e.getMessage());
                    }
                }
            }
        } catch (HyracksDataException e) {
            if (LOGGER.isLoggable(Level.SEVERE)) {
                LOGGER.severe(e.getMessage());
            }
        }
        return null;
    }

    /**
     * @return schedule.
     */
    public HashMap<Integer, String> getSchedule() {
        return this.schedule;
    }

    /**
     * Return the splits belonging to this node for the existing schedule.
     *
     * @param node
     *            HDFS node
     * @return List
     */
    public ArrayList<Integer> getScheduleForNode(String node) {
        ArrayList<Integer> nodeSchedule = new ArrayList<>();
        for (int split : this.schedule.keySet()) {
            if (node.equals(this.schedule.get(split))) {
                nodeSchedule.add(split);
            }
        }
        return nodeSchedule;
    }

    public List<InputSplit> getSplits() {
        return this.splits;
    }

    public Job getJob() {
        return this.job;
    }

    public InputFormat getinputFormat() {
        return this.inputFormat;
    }

    public Document convertStringToDocument(String xmlStr) {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder;
        try {
            builder = factory.newDocumentBuilder();
            Document doc = builder.parse(new InputSource(new StringReader(xmlStr)));
            return doc;
        } catch (Exception e) {
            if (LOGGER.isLoggable(Level.SEVERE)) {
                LOGGER.severe(e.getMessage());
            }
        }
        return null;
    }
}
