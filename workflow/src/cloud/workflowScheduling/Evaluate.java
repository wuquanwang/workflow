package cloud.workflowScheduling;

import java.io.*;
import java.math.*;
import org.apache.commons.math3.stat.*;

import cloud.workflowScheduling.methods.*;
import cloud.workflowScheduling.setting.*;
/*
 * Please download the DAX workflow archive from 
 * https://download.pegasus.isi.edu/misc/SyntheticWorkflows.tar.gz, 
 * unzip it and keep the DAX workflows in an appropriate position before running.
 */
public class Evaluate {
	//because in Java float numbers can not be precisely stored, a very small number E is added before testing whether deadline is met
	public static final double E = 0.0000001; 
	
	//deadline factor.    tight, 0.005:0.005:0.05; loose, 0.05:0.05:0.5
	private static final double DF_START = 0.005, DF_INCR = 0.005, DF_END=0.05;	
	private static final int FILE_INDEX_MAX = 5;
	private static final int[] SIZES = { 50, 100, 500};		//50, 100, 500
	//new ICPCP(), new PSO(), new ProLiS(1.5),new LACO()
	//new ProLiS(1), new ProLiS(1.5), new ProLiS(2), new ProLiS(4), new ProLiS(8), new ProLiS(Double.MAX_VALUE)
	private static final Scheduler[] METHODS = {new ICPCP(), new PSO(), new ProLiS(1.5),new LACO() };
	//"GENOME", "CYBERSHAKE", "LIGO", "MONTAGE"    floodplain是两个两个并行，最后一个输出
	private static final String[] WORKFLOWS = { "GENOME", "CYBERSHAKE", "LIGO", "MONTAGE"};
	
	static final String WORKFLOW_LOCATION = "E:\\dax";
	static final String OUTPUT_LOCATION = "F:";
	
	public static void main(String[] args)throws Exception{
		int deadlineNum = (int)((DF_END-DF_START)/DF_INCR + 1);
		
		for(String workflow : WORKFLOWS){
			//three dimensions of these two arrays correspond to deadlines, methods, files, respectively
			double[][][] successResult = new double[deadlineNum][METHODS.length][FILE_INDEX_MAX * SIZES.length];
			double[][][] NCResult = new double[deadlineNum][METHODS.length][FILE_INDEX_MAX * SIZES.length]; 
			double[] refValues = new double[4];		//store cost and time of fastSchedule and cheapSchedule
			
			for(int di = 0; di<=(DF_END-DF_START)/DF_INCR; di++){	// deadline index
				for(int si = 0; si <SIZES.length; si++){			// size index
					int size = SIZES[si];
					for(int fi = 0;fi<FILE_INDEX_MAX;fi++){			//workflow file index
						String file = WORKFLOW_LOCATION + "\\" + workflow + "\\" + workflow + ".n." + size + "." + fi + ".dax";
						test(file, di, fi, si, successResult, NCResult, refValues);
					}
				}
			}
			BufferedWriter bw = new BufferedWriter(new FileWriter(OUTPUT_LOCATION + "\\" + workflow + ".txt"));
			bw.write("used methods: ");
			for(Scheduler s:METHODS)
				bw.write(s.getClass().getSimpleName()+"\t");
			bw.write("\r\n\r\n");
			printTo(bw, successResult, "success ratio");
			printTo(bw, NCResult, "normalized cost");

			bw.write("reference values (CF, MF, CC, MC)\r\n");
			double divider = SIZES.length * FILE_INDEX_MAX * deadlineNum;
			for(double refValue : refValues)
				bw.write(refValue / divider + "\t");
			bw.close();	
		}
	}
	
	private static void test(String file, int di, int fi, int si, double[][][] successResult,
			double[][][] NCResult, double[] refValues){
		Workflow wf = new Workflow(file);	
		Benchmarks benSched = new Benchmarks(wf);
		System.out.println("Benchmark-FastSchedule：" + benSched.getFastSchedule());
		System.out.println("Benchmark-CheapSchedule：" + benSched.getCheapSchedule());
		
		double deadlineFactor = DF_START + DF_INCR * di; 
		double deadline = benSched.getFastSchedule().calcMakespan() + (benSched.getCheapSchedule().calcMakespan()
				- benSched.getFastSchedule().calcMakespan())* deadlineFactor;

		for(int mi=0;mi<METHODS.length;mi++){		//method index
			Scheduler method = METHODS[mi];
			wf.setDeadline(deadline);	
			System.out.println("The current algorithm: " + method.getClass().getCanonicalName());

			Solution sol = method.schedule(wf);
			if(sol == null)
				continue;
			int isSatisfied = sol.calcMakespan()<=deadline + E ? 1 : 0;
			if(sol.validate(wf) == false)
				throw new RuntimeException();
			System.out.println(sol);
			successResult[di][mi][fi + si*FILE_INDEX_MAX] += isSatisfied;
			NCResult[di][mi][fi + si*FILE_INDEX_MAX] += sol.calcCost() / benSched.getCheapSchedule().calcCost();
		}
		refValues[0]+=benSched.getFastSchedule().calcCost();
		refValues[1]+=benSched.getFastSchedule().calcMakespan();
		refValues[2]+=benSched.getCheapSchedule().calcCost();
		refValues[3]+=benSched.getCheapSchedule().calcMakespan();
	}
	
	private static final java.text.DecimalFormat df = new java.text.DecimalFormat("0.000");
	static {df.setRoundingMode(RoundingMode.HALF_UP); }
	private static void printTo(BufferedWriter bw, double[][][] result, String resultName)throws Exception{
		bw.write(resultName + "\r\n");
		for(int di = 0;di<=(DF_END-DF_START)/DF_INCR;di++){
			String text = df.format(DF_START + DF_INCR * di) + "\t";
			for(int mi=0;mi<METHODS.length;mi++)
				text += df.format(StatUtils.mean(result[di][mi])) + "\t";
			bw.write(text + "\r\n");
			bw.flush();
		}
		bw.write("\r\n\r\n\r\n");
	}
}