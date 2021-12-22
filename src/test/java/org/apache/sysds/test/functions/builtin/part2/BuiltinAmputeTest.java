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


import org.apache.commons.lang.ArrayUtils;
import org.apache.sysds.runtime.lineage.LineageCacheConfig.ReuseCacheType;
import org.apache.sysds.runtime.matrix.data.MatrixValue.CellIndex;
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

    private final static int numSamples = 10000;
	private final static int numFeatures = 11;

    private final double[][] data = getRandomMatrix(numSamples, numFeatures, 1, 10, 1.0, 42);
    private final double defaultProp = 0.5;
    private final double[][] patterns = getPatterns(false);
    private final double[][] frequencies = getFrequencies(true);
    private final double[][] weights = getRandomMatrix(patterns.length, numFeatures, -1.0, 1.0, 0.9, 42);
    private final double[][] type = getTypes();
    

    private double[][] getPatterns(boolean rowOfOnes) {
        // number of patterns = number of features for this one
        // simple pattern using zeros on diagonals (like an X) for amputation
        // param rowOfOnes was used for testing an edge case where patterns without zeros exist
        double [][] tmpPatterns = new double[numFeatures][numFeatures];
        for(int i = 0; i < numFeatures; i++) {
            for (int j = 0; j < numFeatures; j++) {
                if(i != j && i+j != numFeatures - 1) tmpPatterns[i][j] = 1.0;
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

    private double[][] getTypes() {
        double[] allowedTypes = {-2.0, 1.0, 0.0, 1.0};
        double [][] tmpTypes = new double[patterns.length][1];
        int counter = 0;
        for (int row = 0; row < tmpTypes.length; row++) {
            for (int col = 0; col < tmpTypes[row].length; col++) {
                tmpTypes[row][col] = allowedTypes[counter % allowedTypes.length]; 
                counter++;
            }
        }
        return tmpTypes;

    }

    private boolean proportionTest(Double expectedProp, Double sampleProp) {
        // two sided prop test using alpha = 0.05
        // H0: pi = prop
        // H1: pi != prop
        double z_boundary = 1.96;
        double z = (sampleProp - expectedProp) / Math.sqrt(expectedProp*(1-expectedProp)/ numSamples);
        return z < z_boundary;

    }

    @Override
	public void setUp() {
        TestConfiguration tc = new TestConfiguration(TEST_CLASS_DIR, TEST_NAME, new String[]{"Test"});
		addTestConfiguration(TEST_NAME, tc);
	}

    @Test
    public void testAmputeMCAR() {
        Double prop = defaultProp;
        boolean standardized = true;
        boolean continous = true;
        boolean byCases = true;
		runAmputeTest(prop, MissingnessMech.MCAR, standardized, continous, byCases, ExecMode.SINGLE_NODE, false);
		runAmputeTest(prop, MissingnessMech.MCAR, false, continous, byCases, ExecMode.SINGLE_NODE, false);
		runAmputeTest(prop, MissingnessMech.MCAR, standardized, false, byCases, ExecMode.SINGLE_NODE, false);
        // prop has to be lowered for cellwise missingness proportion, 
        // because of limited amount of zeros in patterns
        // is checked in dml script and would trigger stop() call
        prop /= numFeatures;
		runAmputeTest(prop, MissingnessMech.MCAR, standardized, continous, false, ExecMode.SINGLE_NODE, false);
	}

    @Test
	public void testAmputeMAR() {
        Double prop = defaultProp;
		boolean standardized = true;
        boolean continous = true;
        boolean byCases = true;
		runAmputeTest(prop, MissingnessMech.MAR, standardized, continous, byCases, ExecMode.SINGLE_NODE, false);
		runAmputeTest(prop, MissingnessMech.MAR, false, continous, byCases, ExecMode.SINGLE_NODE, false);
		runAmputeTest(prop, MissingnessMech.MAR, standardized, false, byCases, ExecMode.SINGLE_NODE, false);
        // prop has to be lowered for cellwise missingness proportion, 
        // because of limited amount of zeros in patterns
        // is checked in dml script and would trigger stop() call
        prop /= numFeatures;
		runAmputeTest(prop, MissingnessMech.MAR, standardized, continous, false, ExecMode.SINGLE_NODE, false);
	}
    
    @Test
	public void testAmputeMNAR() {
        Double prop = defaultProp;
		boolean standardized = true;
        boolean continous = true;
        boolean byCases = true;
		runAmputeTest(prop, MissingnessMech.MNAR, standardized, continous, byCases, ExecMode.SINGLE_NODE, false);
		runAmputeTest(prop, MissingnessMech.MNAR, false, continous, byCases, ExecMode.SINGLE_NODE, false);
		runAmputeTest(prop, MissingnessMech.MNAR, standardized, false, byCases, ExecMode.SINGLE_NODE, false);
        // prop has to be lowered for cellwise missingness proportion, 
        // because of limited amount of zeros in patterns
        // is checked in dml script and would trigger stop() call
        prop /= numFeatures;
		runAmputeTest(prop, MissingnessMech.MNAR, standardized, continous, false, ExecMode.SINGLE_NODE, false);
	}
    

    private void runAmputeTest(Double prop, MissingnessMech mech, boolean standardized, boolean continous, boolean byCases, ExecMode mode, boolean lineage) {
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
                "type=" + input("type"),
                "byCases=" + String.valueOf(byCases).toUpperCase(),
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
            writeInputMatrixWithMTD("type", type, false);

            runTest(true, EXCEPTION_NOT_EXPECTED, null, -1); 
            MatrixCharacteristics mc = readDMLMetaDataFile("amputedData");
            Assert.assertEquals(numSamples, mc.getRows());
			Assert.assertEquals(numFeatures, mc.getCols());
            
            Double sampleProp = 0.0;
            if (byCases) {
                Double amputedRowCount = readDMLScalarFromOutputDir("amputedRowCount").get(new CellIndex(1,1));
                sampleProp = amputedRowCount / numSamples;
            } else {
                Double amputedCellCount = readDMLScalarFromOutputDir("amputedCellCount").get(new CellIndex(1,1));
                sampleProp = amputedCellCount / (numSamples * numFeatures);
            }
            // works in most cases but then as expected when running too often there will be exceptions
            // since a rather tight two sided alpha of 0.05 is used, but stays really close to expected prop
            // commented out since its probably not a good idea to use it for automated tests
            // Assert.assertTrue("proportion-test", proportionTest(prop, sampleProp));
            
        } finally {
            resetExecMode(execModeOld);
        }
    }
    
}
