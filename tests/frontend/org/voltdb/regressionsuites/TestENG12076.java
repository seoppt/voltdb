/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */package org.voltdb.regressionsuites;

import java.io.IOException;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestENG12076 extends RegressionSuite {

    public TestENG12076(String name) {
        super(name);
        // TODO Auto-generated constructor stub
    }

    private void insertData(Client client) throws Exception {
        ClientResponse cr;
        // INSERT INTO R1
        //   ( ID, INT, VCHAR_INLINE, TIME, VARBIN )
        // VALUES
        //   ( 74, 144, 'r', '1942-11-31 15:45:43.429', x'5D');
        cr = client.callProcedure("R1.insert",
                                  74,   // ID
                                  144,  // INT
                                  "r"   // VCHAR_INLINE
                                  );
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        // INSERT INTO P1
        //   ( ID, INT, VCHAR_INLINE, TIME, VARBIN )
        // VALUES
        //   ( 74, 144, 'r', '1942-11-31 15:45:43.429', x'5D');
        cr = client.callProcedure("P1.insert",
                                  74,   // ID
                                  144,  // INT
                                  "r"   // VCHAR_INLINE
                                  );
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
    }

    public void testENG12076() throws Exception {
        Client client = getClient();
        ClientResponse cr;
        VoltTable vt;

        insertData(client);
        cr = client.callProcedure("@AdHoc",
                                  "SELECT 0 " +
                                  "FROM ( " +
                                  "  SELECT distinct * FROM P1 AS T1, R1 " +
                                  ") T1 LIMIT 2;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
    }
    static private void setupSchema(VoltProjectBuilder project) throws IOException {
        String literalSchema =
            "CREATE TABLE P1 ( " +
            "    ID      INTEGER NOT NULL " +
            "    ,INT     INTEGER " +
            "    ,VCHAR_INLINE     VARCHAR(14) " +
            "); " +
            "PARTITION TABLE P1 ON COLUMN ID; " +
            " " +
            "CREATE TABLE R1 ( " +
            "    ID      INTEGER NOT NULL " +
            "    ,INT     INTEGER " +
            "    ,VCHAR_INLINE     VARCHAR(42 BYTES) " +
            "); " +
            "";
        project.addLiteralSchema(literalSchema);
        project.setUseDDLSchema(true);
    }

    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestENG12076.class);
        boolean success = false;

        VoltProjectBuilder project;
        try {
            project = new VoltProjectBuilder();
            config = new LocalCluster("test-eng12076.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
            setupSchema(project);
            success = config.compile(project);
            assertTrue(success);
            builder.addServerConfig(config);

            /*
            project = new VoltProjectBuilder();
            config = new LocalCluster("test-eng12076.jar", 3, 1, 0, BackendTarget.NATIVE_EE_JNI);
            setupSchema(project);
            success = config.compile(project);
            assertTrue(success);
            builder.addServerConfig(config);
            */
        }
        catch (IOException excp) {
            fail();
        }

        return builder;
    }
}
