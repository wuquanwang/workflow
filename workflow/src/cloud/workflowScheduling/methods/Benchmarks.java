package cloud.workflowScheduling.methods;

import java.util.*;

import cloud.workflowScheduling.setting.*;


public class Benchmarks {

	private Solution cheapSchedule, fastSchedule;

	public Benchmarks(Workflow wf){
		fastSchedule =  bLevelEST(wf);		//VM固定为FASTEST且可以任意多个，求取近似的最快时间
		cheapSchedule = slowestVMEST(wf);	//uses one slowest VM	
	}
	
	// in one slowest VM, use EST to allocate tasks
	private Solution slowestVMEST(Workflow wf){
		Solution solution = new Solution();
		
		VM vm = new VM(VM.SLOWEST);
		for(Task task : wf){
			double EST = solution.calcEST(task, vm);
			solution.addTaskToVM(vm, task, EST, true);
		}
		return solution;
	}
	
	//list scheduling based on bLevel and EST; a kind of HEFT
	private Solution bLevelEST(Workflow wf) {
		Solution solution = new Solution();
		
		List<Task> tasks = new ArrayList<Task>(wf);
		Collections.sort(tasks, new Task.BLevelComparator()); 	//sort based on bLevel
		Collections.reverse(tasks); 	// larger first
		
		for(Task task : tasks){				//select VM based on EST
			double minEST = Double.MAX_VALUE;
			VM selectedVM = null;
			for(VM vm : solution.keySet()){				// calculate EST of task on all the used VMs
				double EST = solution.calcEST(task, vm);
				if(EST<minEST){
					minEST = EST;
					selectedVM = vm;
				}
			}
			//在云中使用需要扩展的点：何时加入新VM
			double EST = solution.calcEST(task, null);	//whether minEST can be shorten if a new vm is added
			if(EST < minEST){
				minEST = EST;
				selectedVM = new VM(VM.FASTEST);
			}
			solution.addTaskToVM(selectedVM, task, minEST, true);	//allocation
		}
		return solution;
	}

	//----------------------------getters-------------------------------------
	public Solution getCheapSchedule() {
		return cheapSchedule;
	}
	public Solution getFastSchedule() {
		return fastSchedule;
	}
}