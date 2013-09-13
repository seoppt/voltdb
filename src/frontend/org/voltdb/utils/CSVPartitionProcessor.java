/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.voltdb.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.voltcore.logging.VoltLogger;
import org.voltdb.TheHashinator;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcedureCallback;
import static org.voltdb.utils.CSVFileReader.synchronizeErrorInfo;

/**
 * Process partition specific data. If the table is not partitioned only one instance of this processor will be used
 *
 */
class CSVPartitionProcessor implements Runnable {

    static public CSVLoader.CSVConfig config;
    final Client csvClient;
    final BlockingQueue<CSVLineWithMetaData> m_partitionQueue;
    final int m_partitionColumnIndex;
    final CSVLineWithMetaData endOfData;
    final int m_partitionId;
    static String insertProcedure = "";
    static String tableName;
    final String m_processorName;
    static Map<Integer, VoltType> columnTypes;
    static VoltTable.ColumnInfo colInfo[];
    static boolean isMP = false;
    final AtomicLong m_partitionProcessedCount = new AtomicLong(0);
    static AtomicLong partitionAcknowledgedCount = new AtomicLong(0);
    protected static final VoltLogger m_log = new VoltLogger("CONSOLE");
    static CountDownLatch processor_cdl;
    boolean m_errored = false;
    static int reportEveryNRows = 10000;

    public CSVPartitionProcessor(Client client, int partitionId,
            int partitionColumnIndex, BlockingQueue<CSVLineWithMetaData> partitionQueue, CSVLineWithMetaData eod) {
        csvClient = client;
        m_partitionId = partitionId;
        m_partitionQueue = partitionQueue;
        m_partitionColumnIndex = partitionColumnIndex;
        endOfData = eod;
        m_processorName = "PartitionProcessor-" + partitionId;
    }

    //Callback for single row procedure invoke called for rows in failed batch.
    public static final class PartitionSingleExecuteProcedureCallback implements ProcedureCallback {

        CSVPartitionProcessor m_processor;
        final CSVLineWithMetaData m_csvLine;

        public PartitionSingleExecuteProcedureCallback(CSVLineWithMetaData csvLine, CSVPartitionProcessor processor) {
            m_processor = processor;
            m_csvLine = csvLine;
        }

        @Override
        public void clientCallback(ClientResponse response) throws Exception {
            if (response.getStatus() != ClientResponse.SUCCESS) {
                String[] info = {m_csvLine.rawLine.toString(), response.getStatusString()};
                if (CSVFileReader.synchronizeErrorInfo(m_csvLine.lineNumber, info)) {
                    m_processor.m_errored = true;
                    return;
                }
                m_log.error(response.getStatusString());
                return;
            }
            long currentCount = CSVPartitionProcessor.partitionAcknowledgedCount.incrementAndGet();

            if (currentCount % reportEveryNRows == 0) {
                m_log.info("Inserted " + currentCount + " rows");
            }
        }
    }
    //Queue of batch entries where some rows failed.
    private BlockingQueue<CSVLineWithMetaData> failedQueue = null;
    private class FailedBatchProcessor extends Thread {

        private final CSVPartitionProcessor m_processor;
        private final String m_procName;
        private final String m_tableName;
        private final Object m_partitionParam;

        public FailedBatchProcessor(CSVPartitionProcessor pp, String procName, String tableName, Object partitionParam) {
            m_processor = pp;
            m_procName = procName;
            m_tableName = tableName;
            m_partitionParam = partitionParam;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    CSVLineWithMetaData lineList;
                    lineList = failedQueue.take();
                    if (lineList == endOfData || m_processor.m_errored) {
                        m_log.info("Shutting down failure processor for  " + m_processor.m_processorName);
                        break;
                    }
                    try {
                        VoltTable table = new VoltTable(colInfo);
                        //No need to check error here if a correctedLine has come here it was previously successful.
                        try {
                            VoltTableUtil.addRowToVoltTableFromLine(table, lineList.correctedLine, columnTypes);
                        } catch (Exception ex) {
                            continue;
                        }

                        PartitionSingleExecuteProcedureCallback cbmt = new PartitionSingleExecuteProcedureCallback(lineList, m_processor);
                        if (!CSVPartitionProcessor.isMP) {
                            csvClient.callProcedure(cbmt, m_procName, m_partitionParam, m_tableName, table);
                        } else {
                            csvClient.callProcedure(cbmt, m_procName, m_tableName, table);
                        }
                        m_partitionProcessedCount.addAndGet(table.getRowCount());
                    } catch (IOException ioex) {
                        m_log.warn("Failure Processor failed, failures will not be processed: " + ioex);
                    }
                } catch (InterruptedException ex) {
                    m_log.info("Stopped failure processor.");
                    break;
                }
            }
        }
    }

    // Callback for batch invoke when table has more than 1 entries. The callback on failure feeds failedQueue for
    // one row at a time processing.
    public static final class PartitionProcedureCallback implements ProcedureCallback {

        static int lastMultiple = 0;
        protected static final VoltLogger m_log = new VoltLogger("CONSOLE");
        protected CSVPartitionProcessor m_processor;
        final private List<CSVLineWithMetaData> m_batchList;
        private final BlockingQueue<CSVLineWithMetaData> failedQueue;

        public PartitionProcedureCallback(List<CSVLineWithMetaData> batchList, CSVPartitionProcessor pp, BlockingQueue<CSVLineWithMetaData> fq) {
            m_processor = pp;
            m_batchList = new ArrayList(batchList);
            failedQueue = fq;
        }

        @Override
        public void clientCallback(ClientResponse response) throws Exception {
            if (response.getStatus() != ClientResponse.SUCCESS) {
                // Batch failed queue it for individual processing and find out which actually m_errored.
                m_processor.m_partitionProcessedCount.addAndGet(-1 * m_batchList.size());
                if (!m_processor.m_errored) {
                    //If we have not reached the limit continue pushing to failure processor.
                    failedQueue.addAll(m_batchList);
                }
                return;
            }
            long executed = response.getResults()[0].asScalarLong();
            long currentCount = CSVPartitionProcessor.partitionAcknowledgedCount.addAndGet(executed);
            int newMultiple = (int) currentCount / reportEveryNRows;
            if (newMultiple != lastMultiple) {
                lastMultiple = newMultiple;
                m_log.info("Inserted " + currentCount + " rows");
            }
        }
    }

    // while there are rows and endOfData not seen batch and call procedure for insert.
    private void processLoadTable(VoltTable table, String procName, Object partitionParam) {
        List<CSVLineWithMetaData> batchList = new ArrayList<CSVLineWithMetaData>();
        while (true) {
            if (m_errored) {
                //Let file reader know not to read any more. All Partition Processors will quit processing.
                CSVFileReader.errored = true;
                return;
            }
            List<CSVLineWithMetaData> mlineList = new ArrayList<CSVLineWithMetaData>();
            m_partitionQueue.drainTo(mlineList, config.batch);
            for (CSVLineWithMetaData lineList : mlineList) {
                if (lineList == endOfData) {
                    //Process anything that we didnt process yet.
                    if (table.getRowCount() > 0) {
                        PartitionProcedureCallback cbmt = new PartitionProcedureCallback(batchList, this, failedQueue);
                        try {
                            if (!isMP) {
                                csvClient.callProcedure(cbmt, procName, partitionParam, tableName, table);
                            } else {
                                csvClient.callProcedure(cbmt, procName, tableName, table);
                            }
                            m_partitionProcessedCount.addAndGet(table.getRowCount());
                        } catch (IOException ex) {
                            String[] info = {lineList.rawLine.toString(), ex.toString()};
                            m_errored = synchronizeErrorInfo(lineList.lineNumber, info);
                        }
                    }
                    return;
                }
                //Build table or just call one proc at a time.
                try {
                    if (VoltTableUtil.addRowToVoltTableFromLine(table, lineList.correctedLine, columnTypes)) {
                        batchList.add(lineList);
                    } else {
                        String[] info = {lineList.rawLine.toString(), "Missing or Invalid Data in Row."};
                        m_errored = synchronizeErrorInfo(lineList.lineNumber, info);
                        continue;
                    }
                } catch (Exception ex) {
                    //Failed to add row....things like larger than supported row size
                    String[] info = {lineList.rawLine.toString(), ex.toString()};
                    m_errored = synchronizeErrorInfo(lineList.lineNumber, info);
                    continue;
                }
                //If our batch is complete submit it.
                if (table.getRowCount() >= config.batch) {
                    try {
                        PartitionProcedureCallback cbmt = new PartitionProcedureCallback(batchList, this, failedQueue);
                        if (!isMP) {
                            csvClient.callProcedure(cbmt, procName, partitionParam, tableName, table);
                        } else {
                            csvClient.callProcedure(cbmt, procName, tableName, table);
                        }
                        m_partitionProcessedCount.addAndGet(table.getRowCount());
                        //Clear table data as we start building new table with new rows.
                        table.clearRowData();
                    } catch (IOException ex) {
                        table.clearRowData();
                        String[] info = {lineList.rawLine.toString(), ex.toString()};
                        m_errored = synchronizeErrorInfo(lineList.lineNumber, info);
                        return;
                    }
                    batchList = new ArrayList<CSVLineWithMetaData>();
                }
            }
        }
    }

    // while there are rows and endOfData not seen batch and call procedure supplied by user.
    private void processUserSuppliedProcedure(String procName) {
        while (true) {
            if (m_errored) {
                //Let file reader know not to read any more. All Partition Processors will quit processing.
                CSVFileReader.errored = true;
                return;
            }
            List<CSVLineWithMetaData> mlineList = new ArrayList<CSVLineWithMetaData>();
            m_partitionQueue.drainTo(mlineList, config.batch);
            for (CSVLineWithMetaData lineList : mlineList) {
                if (lineList == endOfData) {
                    return;
                }
                // call supplied procedure.
                try {
                    PartitionSingleExecuteProcedureCallback cbmt = new PartitionSingleExecuteProcedureCallback(lineList, this);
                    csvClient.callProcedure(cbmt, procName, (Object[]) lineList.correctedLine);
                    m_partitionProcessedCount.incrementAndGet();
                } catch (IOException ex) {
                    String[] info = {lineList.rawLine.toString(), ex.toString()};
                    m_errored = synchronizeErrorInfo(lineList.lineNumber, info);
                    return;
                }
            }
        }
    }

    @Override
    public void run() {

        FailedBatchProcessor failureProcessor = null;
        //Process the Partition queue.
        if (config.useSuppliedProcedure) {
            processUserSuppliedProcedure(insertProcedure);
        } else {
            //If SP get partition param from Hashinator.
            Object partitionParam = null;
            if (!isMP) {
                partitionParam = TheHashinator.valueToBytes(m_partitionId);
            }

            VoltTable table = new VoltTable(colInfo);
            String procName = (isMP ? "@LoadMultipartitionTable" : "@LoadSinglepartitionTable");

            //Launch failureProcessor
            failedQueue = new LinkedBlockingQueue<CSVLineWithMetaData>();
            failureProcessor = new FailedBatchProcessor(this, procName, tableName, partitionParam);
            failureProcessor.start();

            processLoadTable(table, procName, partitionParam);
        }
        m_partitionQueue.clear();

        //Let partition processor drain and put any failures on failure processing.
        try {
            csvClient.drain();
            if (failureProcessor != null) {
                failedQueue.put(endOfData);
                failureProcessor.join();
                //Drain again for failure callbacks to finish.
                csvClient.drain();
            }
        } catch (NoConnectionsException ex) {
            m_log.warn("Failed to Drain the client: ", ex);
        } catch (InterruptedException ex) {
            m_log.warn("Failed to Drain the client: ", ex);
        }

        CSVPartitionProcessor.processor_cdl.countDown();
        m_log.info("Done Processing partition: " + m_partitionId + " Processed: " + m_partitionProcessedCount);
    }
}
