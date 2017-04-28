package cloud.workflowScheduling.setting;

import java.util.*;

import cloud.workflowScheduling.*;


//Allocation List is sorted based on startTime
public class Solution extends HashMap<VM, LinkedList<Allocation>>{

	private static final long serialVersionUID = 1L;
	
	//the content in revMapping is the same as that in HashMap<VM, LinkedList<Allocation>>
	//used to make get_Allocation_by_Task easy 
	private HashMap<Task, Allocation> revMapping = new HashMap<Task, Allocation>();	//reverseMapping
	
	public Solution() {
		super();
		VM.resetInternalId();
	}
	
	//----------------------------------------add a task-------------------------------------------
	//isEnd denotes whether the task is placed at the end, or the beginning
	public void addTaskToVM(VM vm, Task task, double startTime, boolean isEnd){
		if(this.containsKey(vm) == false)
			this.put(vm, new LinkedList<Allocation>());
		
		Allocation alloc = new Allocation(vm, task, startTime);
		
		boolean conflict = false;				//check whether there is time conflict
		double startTime1 = alloc.getStartTime();
		double finishTime1 = alloc.getFinishTime();
		for(Allocation prevAlloc : this.get(vm)){
			double startTime2 = prevAlloc.getStartTime();
			double finishTime2 = prevAlloc.getFinishTime();
			if((startTime1>startTime2 && startTime2>finishTime1)	  //startTime2 is between startTime1 and finishTime1
				|| (startTime2>startTime1 && startTime1>finishTime2)) //startTime1 is between startTime2 and finishTime2
				conflict = true;
		}
		if(conflict)
			throw new RuntimeException("Critical Error: Allocation conflicts");

		if(isEnd)
			this.get(vm).add(alloc);
		else
			this.get(vm).add(0, alloc);
		revMapping.put(alloc.getTask(), alloc);
	}
	

	
	public void updateVM(VM vm){		//这里仅进行该VM上的更新，其他的vm的就不再涉及了
		vm.setType(vm.getType()+1);
		
		LinkedList<Allocation> list = this.get(vm);
		if(list == null)
			return;
		for(Allocation alloc : list){
			double newFinishTime = alloc.getTask().getTaskSize() / vm.getSpeed() + alloc.getStartTime();
			alloc.setFinishTime(newFinishTime);
		}
	}

	//----------------------------------------calculations-------------------------------------------
	//calculate Earliest Starting Time of task on vm	
	public double calcEST(Task task, VM vm){
		double EST = 0; 			
		for(Edge inEdge : task.getInEdges()){
			Task parent = inEdge.getSource();
			Allocation alloc = revMapping.get(parent);
			VM parentVM = alloc.getVM();
			double arrivalTime = alloc.getFinishTime();
			if( parentVM != vm )
				arrivalTime += inEdge.getDataSize() / VM.NETWORK_SPEED;
			EST = Math.max(EST, arrivalTime);
		}
		if(vm == null)
			EST = Math.max(EST, VM.LAUNCH_TIME);
		else
			EST = Math.max(EST, this.getVMReadyTime(vm));
		return EST;
	}
	
	public double calcCost(){
		double totalCost = 0;
		for(VM vm : this.keySet()){
			double vmCost = calcVMCost(vm); 
			totalCost += vmCost;
		}
		return totalCost;
	}
	public double calcVMCost(VM vm){
		return vm.getUnitCost() * Math.ceil((this.getVMLeaseEndTime(vm) - this.getVMLeaseStartTime(vm))/VM.INTERVAL);
	}
	
	
	public double calcMakespan(){
		double makespan = -1;
		for(VM vm : this.keySet()){
			double finishTime = this.getVMReadyTime(vm);	//finish time of the last task
			if(finishTime > makespan)
				makespan = finishTime;
		}
		return makespan;
	}

	// compare this solution to Solution s; if ==, returns false; used by ACO, PSO
	public boolean isBetterThan(Solution s, double epsilonDeadline){
		double makespan1 = this.calcMakespan();
		double makespan2 = s.calcMakespan();
		double cost1 = this.calcCost();
		double cost2 = s.calcCost();
		
		if(makespan1 <= epsilonDeadline && makespan2<= epsilonDeadline ){	//both satisfy deadline
			return cost1<cost2;
		}else if(makespan1 > epsilonDeadline && makespan2 > epsilonDeadline ){//both does not satisfy
			return makespan1<makespan2;
		}else if(makespan1 <= epsilonDeadline && makespan2 > epsilonDeadline){ //this satisfy，s doesn't
			return true;
		}else if(makespan1 > epsilonDeadline && makespan2 <= epsilonDeadline) //this don't，s satisfies
			return false;
		
		return true;
	}
	//check whether there is time conflict in this schedule solution
	public boolean validate(Workflow wf){
		List<Allocation> list = new ArrayList<Allocation>(revMapping.values());
		
		Set<Task> set = new HashSet<Task>();
		for(Allocation alloc : list)
			set.add(alloc.getTask());
		if(set.size() != wf.size())	{	//check # of tasks
			return false;
		}
		
//		Collections.sort(list);			//把该solution当中的task以时间顺序排列，并检测是否是拓扑排序
		for(Allocation alloc : list){
			Task task = alloc.getTask();	// check each task and its children
			for(Edge e : task.getOutEdges()){
				Task child = e.getDestination();
				
				Allocation childAlloc = this.revMapping.get(child);
				boolean isValid = false;
				if(alloc.getVM() != childAlloc.getVM() 				
						&& alloc.getFinishTime() +e.getDataSize()/VM.NETWORK_SPEED <= childAlloc.getStartTime()+ Evaluate.E)
					isValid = true;
				else if(alloc.getVM() == childAlloc.getVM() 				
						&& alloc.getFinishTime() <= childAlloc.getStartTime() + Evaluate.E)
					isValid = true;
				if(isValid == false)
					return false;
			}
		}
		return true;
	}
		
	//----------------------------------------getters-------------------------------------------
	//VM's lease start time and finish time are calculated based on allocations
	public double getVMLeaseStartTime(VM vm){	
		if(this.get(vm).size() == 0)
			return VM.LAUNCH_TIME;
		else{
			Task firstTask = this.get(vm).get(0).getTask();
			double ftStartTime = this.get(vm).get(0).getStartTime(); // startTime of first task
			
			double maxTransferTime = 0;
			for(Edge e : firstTask.getInEdges()){
				Allocation alloc = revMapping.get(e.getSource());
				if(alloc == null || alloc.getVM() != vm)		// parentTask's VM != vm
					maxTransferTime = Math.max(maxTransferTime, e.getDataSize() / VM.NETWORK_SPEED);
			}
			return ftStartTime - maxTransferTime;
		}
	}
	public double getVMLeaseEndTime(VM vm){
		if(this.get(vm)== null || this.get(vm).size() == 0)
			return VM.LAUNCH_TIME;
		else{
			LinkedList<Allocation> allocations = this.get(vm);
			
			Task lastTask = allocations.get(allocations.size()-1).getTask();
			double ltFinishTime = allocations.get(allocations.size()-1).getFinishTime(); // finishTime of last task
			
			double maxTransferTime = 0;
			for(Edge e : lastTask.getOutEdges()){
				Allocation alloc = revMapping.get(e.getDestination());
				if(alloc == null || alloc.getVM() != vm)		// childTask's VM != vm
					maxTransferTime = Math.max(maxTransferTime, e.getDataSize() / VM.NETWORK_SPEED);
			}
			return ltFinishTime + maxTransferTime;
		}
	}
	//note the difference between VMReadyTime and VMLeaseEndTime.
	public double getVMReadyTime(VM vm){		//finish time of the last task
		if(this.get(vm)== null || this.get(vm).size() == 0)
			return VM.LAUNCH_TIME;
		else{
			LinkedList<Allocation> allocations = this.get(vm);
			return allocations.get(allocations.size()-1).getFinishTime(); 
		}
	}
	public HashMap<Task, Allocation> getRevMapping() {
		return revMapping;
	}

	//----------------------------------------override-------------------------------------------
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("required cost：" + this.calcCost() + "\trequired time：" + this.calcMakespan()+"\r\n");
		for(VM vm : this.keySet()){
			sb.append(vm.toString() + this.get(vm).toString()+"\r\n");
		}
		return sb.toString();
	}
	
	// ----------------------------------these three functions only used by ICPCP-------------------

}