package cloud.workflowScheduling.methods;

import static java.lang.Math.*;
import java.util.*;

import cloud.workflowScheduling.setting.*;

public class LACO implements Scheduler {
	private static final double ALPHA = 1;
	private static final double BETA = 2;
	private static final double EVAP_RATIO = 0.8;
	private static final int NO_OF_ITE = 50;
	private static final int NO_OF_EPSILON_ITE = (int)(NO_OF_ITE*0.7);
	private static final int NO_OF_ANTS = 20;
	
	private double[][] pheromone; 
	private double[] heuristic;
	private Workflow wf;
	private ProLiS pds = new ProLiS(1.5);
	
	private double epsilonDeadline;
	
	@Override
	public Solution schedule(Workflow wf) {
		this.wf = wf;
		int size = wf.size();
		heuristic = new double[size];
		pheromone = new double[size][size];
		for(int i =0;i<size;i++)		//initialize pheromone
			for(int j=0;j<size;j++)
				pheromone[i][j] = 1;

		Benchmarks bench = new Benchmarks(wf);
		double maxMakespan = bench.getCheapSchedule().calcMakespan();//used to calculate epsilonDeadline
		Ant gbAnt = null;	//globalBestAnt
		for(int iterIndex = 0; iterIndex<NO_OF_ITE; iterIndex++){	 //iteration index
			Ant[] ants = new Ant[NO_OF_ANTS];
			for(Task t : wf)	//initialize heuristic information
				heuristic[t.getId()] = t.getpURank();
			
			if(maxMakespan<wf.getDeadline() || iterIndex >= NO_OF_EPSILON_ITE)
				epsilonDeadline = wf.getDeadline();
			else
				epsilonDeadline = wf.getDeadline() +
					(maxMakespan-wf.getDeadline())* Math.pow((1-(double)iterIndex/NO_OF_EPSILON_ITE), 4);
			Ant lbAnt = null;	//localBestAnt
			for(int antId = 0;antId<NO_OF_ANTS;antId++){
				ants[antId] = new Ant();
				ants[antId].constructASolution();
				if(lbAnt==null || ants[antId].solution.isBetterThan(lbAnt.solution, epsilonDeadline))
					lbAnt = ants[antId];
			}
			
			//update  pheromone
			for(int j =0;j<size;j++)	
				for(int i=0;i<size;i++)
					pheromone[j][i] *= EVAP_RATIO;
			if(gbAnt!=null && random()>0.9)
				gbAnt.releasePheromone();
			else
				lbAnt.releasePheromone();
			for(int j =0;j<size;j++){
				for(int i=0;i<size;i++){
					if(pheromone[j][i]>1)
						pheromone[j][i]=1;
					else if(pheromone[j][i]<0.2)
						pheromone[j][i]=0.2;
				}
			}
			
			if(gbAnt==null || lbAnt.solution.isBetterThan(gbAnt.solution, epsilonDeadline)){
				gbAnt = lbAnt;
				System.out.printf("Iteration index£º%3d\t%5.2f\t%5.2f\t%5.2f\n",iterIndex,
						gbAnt.getSolution().calcCost(),
						gbAnt.getSolution().calcMakespan(),epsilonDeadline);
			}
		}
		return gbAnt.getSolution();
	}
	
    private class Ant {
		private Solution solution;
		private int[] taskIdList = new int[wf.size()];
		
		public Ant(){
			wf.calcPURank(pds.getTheta());
		}
		
		public Solution constructASolution(){
    		List<Task> L = new ArrayList<Task>();	//Empty list that will contain the sorted elements
    		List<Task> S = new ArrayList<Task>();	//S: Set of all nodes with no incoming edges	
    		S.add(wf.get(0));		

    		for(Task t : wf)		//set topoCount to 0
    			t.setTopoCount(0);
    		
    		int tIndex = 0;			//task index in task ordering L
    		while(S.size()>0){
    			Task task;       
    			// remove a task from S
    			if(tIndex==0)	
    				task = S.remove(0);	//entry task
    			else
    				task = chooseNextTask(taskIdList[tIndex], S);
    			
    			taskIdList[tIndex] = task.getId();
    			tIndex++;
    			L.add(task);					// add n to tail of L
        		
    			for(Edge e : task.getOutEdges()){	// for each node m with an edge e from n to m do
    				Task child = e.getDestination();
    				child.setTopoCount(child.getTopoCount()+1);//remove edge e from the graph--achieved by setting TopoCount here
    				if(child.getTopoCount() == child.getInEdges().size())	//  if m has no other incoming edges then
    					S.add(child);					// insert m into S			
    			}
    		}

    		solution =  pds.buildViaTaskList(wf, L, epsilonDeadline);
    		return solution;
    	}
        
        private Task chooseNextTask(int curTaskId, List<Task> S) {
            double sum = 0;		
            for (Task t : S)	
                sum += pow(pheromone[curTaskId][t.getId()], ALPHA) * pow(heuristic[t.getId()], BETA);
            
            double slice = sum * random();
            double k = 0;			
            int chosenIndex = 0;			//the chosen index in S
            for (int indexInS = 0; k < slice; indexInS++) {	
            	Task t = S.get(indexInS);
                k += pow(pheromone[curTaskId][t.getId()], ALPHA) * pow(heuristic[t.getId()], BETA);
                chosenIndex = indexInS;
            }
            return S.remove(chosenIndex);
        }
    	
        public void releasePheromone() {
        	double value = 1 / solution.calcCost() + 0.5;
        	for(int i = 0;i<taskIdList.length-1; i++)
        		pheromone[taskIdList[i]][taskIdList[i+1]] += value;
        }

    	public Solution getSolution() {
			return solution;
		}

		@Override
		public String toString() {
			return "Ant [cost=" + solution.calcCost() + ", makespan=" + solution.calcMakespan()+ "]";
		}
    }
}