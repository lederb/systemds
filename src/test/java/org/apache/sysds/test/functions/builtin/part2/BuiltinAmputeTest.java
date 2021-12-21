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
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.I;
import org.apache.sysds.runtime.lineage.LineageCacheConfig.ReuseCacheType;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.meta.MetaData;
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

    private final static int numSamples = 10000;
	private final static int numFeatures = 14;

    private final double[][] data = getRandomMatrix(numSamples, numFeatures, 1, 10, 1.0, 42);
    private final double prop = 0.25;
    private final double[][] patterns = getPatterns(false);
    private final double[][] frequencies = getFrequencies(true);
    private final double[][] weights = getRandomMatrix(patterns.length, numFeatures, -1.0, 1.0, 0.9, 42);
    

    private double[][] getPatterns(boolean rowOfOnes) {
        double [][] tmpPatterns = new double[numFeatures][numFeatures];
        for(int i = 0; i < numFeatures; i++) {
            for (int j = 0; j < numFeatures; j++) {
                if(i != j) tmpPatterns[i][j] = 1.0;
                if (rowOfOnes && i == 0) {
                    tmpPatterns[i][j] = 1.0;
                }
            }
            
        }
        return tmpPatterns;
    }

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
        TestConfiguration tc = new TestConfiguration(TEST_CLASS_DIR, TEST_NAME, new String[]{"Test"});
		addTestConfiguration(TEST_NAME, tc);
	}

    @Test
    public void testAmputeMCARstd() {
        boolean standardized = true;
        boolean continous = true;
		runAmputeTest(MissingnessMech.MCAR,  standardized, continous, ExecMode.SINGLE_NODE, false);
	}

    @Test
    public void testAmputeMCARnonStd() {
        boolean standardized = false;
        boolean continous = true;
		runAmputeTest(MissingnessMech.MCAR,  standardized, continous, ExecMode.SINGLE_NODE, false);
	}
    
    @Test
	public void testAmputeMARstdDiscrete() {
		boolean standardized = true;
        boolean continous = false;
		runAmputeTest(MissingnessMech.MAR,  standardized, continous, ExecMode.SINGLE_NODE, false);
	}
    
    @Test
    public void testAmputeMARnonStdDiscrete() {
		boolean standardized = false;
        boolean continous = false;
		runAmputeTest(MissingnessMech.MAR,  standardized, continous, ExecMode.SINGLE_NODE, false);
	}

    @Test
	public void testAmputeMNARstdDiscrete() {
		boolean standardized = true;
        boolean continous = false;
		runAmputeTest(MissingnessMech.MNAR,  standardized, continous, ExecMode.SINGLE_NODE, false);
	}
    
    @Test
	public void testAmputeMNARnonStdDiscrete() {
		boolean standardized = false;
        boolean continous = false;
		runAmputeTest(MissingnessMech.MNAR,  standardized, continous, ExecMode.SINGLE_NODE, false);
	}
    
    @Test
	public void testAmputeMARstdContinous() {
		boolean standardized = true;
        boolean continous = true;
		runAmputeTest(MissingnessMech.MAR,  standardized, continous, ExecMode.SINGLE_NODE, false);
	}
    
    @Test
	public void testAmputeMARnonStdContinous() {
		boolean standardized = false;
        boolean continous = true;
		runAmputeTest(MissingnessMech.MAR,  standardized, continous, ExecMode.SINGLE_NODE, false);
	}

    @Test
	public void testAmputeMNARstdContinous() {
		boolean standardized = true;
        boolean continous = true;
		runAmputeTest(MissingnessMech.MNAR,  standardized, continous, ExecMode.SINGLE_NODE, false);
	}
    
    @Test
	public void testAmputeMNARnonStdContinous() {
		boolean standardized = false;
        boolean continous = true;
		runAmputeTest(MissingnessMech.MNAR,  standardized, continous, ExecMode.SINGLE_NODE, false);
	}


    private void runAmputeTest(MissingnessMech mech, boolean standardized, boolean continous, ExecMode mode, boolean lineage) {
        ExecMode execModeOld = setExecMode(mode);
        try {
            loadTestConfiguration(getTestConfiguration(TEST_NAME));
            String HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = HOME + TEST_NAME + ".dml";
            programArgs = new String[]{
                "-nvargs", 
                "data="+input("data"),
                "prop="+String.valueOf(prop), 
                "patterns=" + input("patterns"),
                "freq=" + input("freq"),
                "mech=" + mech.getMech(),
                "weights=" + input("weights"),
                "std=" + String.valueOf(standardized).toUpperCase(),
                "cont=" + String.valueOf(continous).toUpperCase(),
                "amputedData=" + output("amputedData"),
                "nanMask=" + output("nanMask"),
                "amputedRowCount=" + output("amputedRowCount"),
                "amputedCellCount=" + output("amputedCellCount"),
            };
			if (lineage) {
				programArgs = (String[]) ArrayUtils.addAll(programArgs, new String[] {
					"-stats","-lineage", ReuseCacheType.REUSE_HYBRID.name().toLowerCase()});
			}
            writeInputMatrixWithMTD("data", data, false);
            writeInputMatrixWithMTD("patterns", patterns, false);
            writeInputMatrixWithMTD("freq", frequencies, false);
            writeInputMatrixWithMTD("weights", weights, false);

            runTest(true, EXCEPTION_NOT_EXPECTED, null, -1);
            MatrixCharacteristics mc = readDMLMetaDataFile("amputedData");
            // System.out.print(readDMLScalarFromOutputDir("amputedRowCount").toString());
            // System.out.print(readDMLScalarFromOutputDir("amputedCellCount").toString());
            Assert.assertEquals(numSamples, mc.getRows());
			Assert.assertEquals(numFeatures, mc.getCols());

        } finally {
            resetExecMode(execModeOld);
        }
    }
    
}
