/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sysds.test.functions.builtin.part2;

import org.apache.sysds.common.Types.ExecMode;

import java.util.Arrays;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.math3.linear.MatrixUtils;

import org.apache.sysds.runtime.lineage.LineageCacheConfig.ReuseCacheType;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.test.AutomatedTestBase;
import org.apache.sysds.test.TestConfiguration;
import org.junit.Assert;
import org.junit.Test;


public class BuiltinAmputeTest extends AutomatedTestBase {

    private enum MissingnessMech {
        MCAR("MCAR"),
        MAR("MAR"),
        MNAR("MNAR");
        private String mech;

        MissingnessMech(String mech) {
            this.mech = mech;
        }

        public String getMech(){
            return mech;
        }

    }

    private final static String TEST_NAME = "ampute";
	private final static String TEST_DIR = "functions/builtin/";
	private static final String TEST_CLASS_DIR = TEST_DIR + BuiltinAmputeTest.class.getSimpleName() + "/";

    private final static int numSamples = 3000;
	private final static int numFeatures = 10;

    private final double[][] D = getRandomMatrix(numSamples, numFeatures, 0.0, 1.0, 1.0, 42);
    private final double prop = 0.2;
    private final double[][] patterns = MatrixUtils.createRealIdentityMatrix(numFeatures).getData();
    private final double[][] frequencies = getFrequencies(true);
    private final double[][] weights = getRandomMatrix(patterns.length, numFeatures, -1.0, 1.0, 0.2, 42);
    
    private double [][] getFrequencies(boolean equal) {
        if (equal) {
            double[][] tmpFreq = new double[patterns.length][1];
            for (int row = 0; row < tmpFreq.length; row++) {
                for (int col = 0; col < tmpFreq[row].length; col++){
                    tmpFreq[row][col] = 1.0/patterns.length;
                }
            }
            return tmpFreq;
        }
        else {
            double[][] tmpFreq = getRandomMatrix(patterns.length, 1, 0.0, 1.0, 1.0, 42);
            double sum = 0.0;
            for (int row = 0; row < tmpFreq.length; row++) {
                for (int col = 0; col < tmpFreq[row].length; col++){
                    sum += tmpFreq[row][col];
                }
            }
            for (int row = 0; row < tmpFreq.length; row++) {
                for (int col = 0; col < tmpFreq[row].length; col++){
                    tmpFreq[row][col] = tmpFreq[row][col]/sum;
                }
            }
            return tmpFreq;
        }
    }

    @Override
	public void setUp() {
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME, new String[]{"B"}));
	}

    @Test
	public void testAmputeMARCP() {
		runAmputeTest(MissingnessMech.MAR, ExecMode.SINGLE_NODE, false);
	}


    private void runAmputeTest(MissingnessMech mech, ExecMode mode, boolean lineage) {
        ExecMode execModeOld = setExecMode(mode);
        try {
            loadTestConfiguration(getTestConfiguration(TEST_NAME));
            String HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = HOME + TEST_NAME + ".dml";
            programArgs = new String[]{
                "-nvargs", 
                "data="+input("D"),
                "prop="+String.valueOf(prop), 
                "patterns=" + input("P"),
                "freq=" + input("F"),
                "mech=" + mech.getMech(),
                "weights=" + input("W"),
                "amputedData=" + output("AMPD"),
            };
			if (lineage) {
				programArgs = (String[]) ArrayUtils.addAll(programArgs, new String[] {
					"-stats","-lineage", ReuseCacheType.REUSE_HYBRID.name().toLowerCase()});
			}
            writeInputMatrixWithMTD("D", D, false);
            writeInputMatrixWithMTD("P", patterns, false);
            writeInputMatrixWithMTD("F", frequencies, false);
            writeInputMatrixWithMTD("W", weights, false);

            runTest(true, EXCEPTION_NOT_EXPECTED, null, -1);
            MatrixCharacteristics mc = readDMLMetaDataFile("AMPD");
            Assert.assertEquals(numSamples, mc.getRows());
			Assert.assertEquals(numFeatures, mc.getCols());

        } finally {
            resetExecMode(execModeOld);
        }
    }
    
}
