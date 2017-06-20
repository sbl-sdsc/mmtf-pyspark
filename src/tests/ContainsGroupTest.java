package edu.sdsc.mmtf.spark.filters;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfReader;

public class ContainsGroupTest {
	private JavaSparkContext sc;
	private JavaPairRDD<String, StructureDataInterface> pdb;
	
	@Before
	public void setUp() throws Exception {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(ContainsGroupTest.class.getSimpleName());
	    sc = new JavaSparkContext(conf);
	    
	    // 1STP: only L-protein chain
	    // 1JLP: single L-protein chains with non-polymer capping group (NH2)
	    // 5X6H: L-protein and DNA chain
	    // 5L2G: DNA chain
	    // 2MK1: D-saccharide
	    List<String> pdbIds = Arrays.asList("1STP","1JLP","5X6H","5L2G","2MK1");
	    pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc);
	}

	@After
	public void tearDown() throws Exception {
		sc.close();
	}

	@Test
	public void test1() {
	    pdb = pdb.filter(new ContainsGroup("BTN"));    
	    List<String> results = pdb.keys().collect();
	    
	    assertTrue(results.contains("1STP"));
	    assertFalse(results.contains("1JLP"));
	    assertFalse(results.contains("5X6H"));
	    assertFalse(results.contains("5L2G"));
	    assertFalse(results.contains("2MK1"));
	}
	
	@Test
	public void test2() {
	    pdb = pdb.filter(new ContainsGroup("HYP"));   
	    List<String> results = pdb.keys().collect();
	    
	    assertFalse(results.contains("1STP"));
	    assertTrue(results.contains("1JLP"));
	    assertFalse(results.contains("5X6H"));
	    assertFalse(results.contains("5L2G"));
	    assertFalse(results.contains("2MK1"));
	}
	
	@Test
	public void test3() {
	    pdb = pdb.filter(new ContainsGroup("HYP","NH2"));   
	    List<String> results = pdb.keys().collect();
	    
	    assertFalse(results.contains("1STP"));
	    assertTrue(results.contains("1JLP"));
	    assertFalse(results.contains("5X6H"));
	    assertFalse(results.contains("5L2G"));
	    assertFalse(results.contains("2MK1"));
	}
}
