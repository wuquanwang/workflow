package cloud.workflowScheduling.methods;

import java.util.*;

import cloud.workflowScheduling.*;
import cloud.workflowScheduling.setting.*;


/*Abrishami, Saeid, Mahmoud Naghibzadeh, and Dick HJ Epema. "Deadline-constrained workflow scheduling algorithms 
  for Infrastructure as a Service Clouds." Future Generation Computer Systems 29.1 (2013): 158-169.*/
public class ICPCP implements Scheduler {
	
	private final double bestVMSpeed = VM.SPEEDS[VM.FASTEST];
	private Workflow wf;
	private Solution solution ;
	
	public Solution schedule(Workflow wf) {
		this.wf = wf;
		this.solution = new Solution();
		try{
			init();									// init
			assignParents(wf.get(wf.size() - 1));	// parent assign for exit task
	
			// allocate entry and exit tasks
			solution.addTaskToVM(getEarliestVM(), wf.get(0), 0, false);
			solution.addTaskToVM(getLatestVM(), wf.get(wf.size()-1), solution.calcMakespan(), true);
			
			return solution;
		}catch(RuntimeException e){
			//it means ICPCP fails to yield a solution meeting the deadline. This is because 'assignPath' may fail
			return null;
		}
	}
	
	private void init(){					//Algorithm 1 in the paper; for cases of initialization and update
		Task entryTask = wf.get(0);
		entryTask.setAST(0);
		entryTask.setAFT(0);
		entryTask.setAssigned(true);
		
		for(int i=1; i<wf.size(); i++){		// compute EST, EFT, critical parent via Eqs. 1 and 2; skip entry task
			Task task = wf.get(i);

			//此处EST定义不考虑resource的available time，且还要计算critical parent；所以没有使用solution.calcEST方法
			double EST = -1;
			double ESTForCritical = -1;
			Task criticalParent = null;		
			for(Edge e: task.getInEdges()){
				Task parent = e.getSource();
				double startTime = e.getDataSize()/VM.NETWORK_SPEED;
				//if assigned, use AFT; otherwise, use EFT
				startTime += parent.isAssigned() ? parent.getAFT() : parent.getEFT();
				EST = Math.max(EST, startTime);				//determine EST
				if(startTime > ESTForCritical && parent.isAssigned()==false){	//determine critical parent
					ESTForCritical = startTime;
					criticalParent = parent;
				}
			}
			if(task.isAssigned() == false){
				task.setEST(EST);
				task.setEFT(EST + task.getTaskSize() / bestVMSpeed);
			}
			//分配了的还需要更新critical parent:因为task a在assignParents，可能有两个parent b和c，所以必须要更新了
			task.setCriticalParent(criticalParent);	
		}

		Task exitTask = wf.get(wf.size()-1);	//Note, EST, EFT, critialParent of exitTask have been set above
		exitTask.setAFT(wf.getDeadline());
		exitTask.setAST(wf.getDeadline());
		exitTask.setAssigned(true);
		for(int j = wf.size() - 2; j>=0; j--){	// compute LFT via Eq. 3; reverse order, skip exit node
			Task task = wf.get(j);
			if(task.isAssigned())
				continue;
			
			double lft = Double.MAX_VALUE;
			for(Edge e : task.getOutEdges()){
				Task child = e.getDestination();
				double finishTime;
				if(child.isAssigned())	
					finishTime = child.getAST() - e.getDataSize() / VM.NETWORK_SPEED;
				else
					finishTime = child.getLFT() - child.getTaskSize()/bestVMSpeed - e.getDataSize() / VM.NETWORK_SPEED;
				lft = Math.min(lft, finishTime);
			}
			task.setLFT(lft);
		}
	}
	
	private void assignParents(Task task){			//Algorithm 2 in the paper
		while(task.getCriticalParent() != null){	
			List<Task> PCP = new ArrayList<Task>();
			Task ti = task;
			while(ti.getCriticalParent() != null){		// while (there exists an unassigned parent of ti)
				PCP.add(0, ti.getCriticalParent());   	//add CriticalParent(ti) to the beginning of PCP
				ti = ti.getCriticalParent();
			}
			assignPath(PCP);	//path assign
			init();				//re-init, i.e., update in the paper
			for(Task tj : PCP)	//call AssignParents(ti)
				assignParents(tj);
		}
	}
	
	//choose the cheapest service for PCP; 从existing和new VM中一起寻找最便宜的；论文里的只要existing里找到就停止
	private void assignPath(List<Task> PCP){	//Algorithm 3 in the paper; the actual situation is more complex
		double minExtraCost = Double.MAX_VALUE;	//the criterion to select VM
		List<Allocation> bestList = null;
		
aa:		for(VM vm : solution.keySet()){			//search from existing VMs. 必要条件：1.使用时间不冲突；2.满足LFT
			List<Allocation> tmpList = new ArrayList<Allocation>();
			for(int i = 0; i<PCP.size(); i++){		
				Task task = PCP.get(i);
				double taskEST = task.getEST();	
				if(i > 0)
					taskEST = Math.max(taskEST, tmpList.get(i-1).getFinishTime());
				if(taskEST + task.getTaskSize() / vm.getSpeed() > task.getLFT() + Evaluate.E)//lft is not met, skip vm
					continue aa;
				
				double startTime = searchStartTime(vm, task, taskEST, task.getLFT());	//how to put task onto vm
				if(startTime != -1)
					tmpList.add(new Allocation(vm, task, startTime));
				else
					continue aa;
			}
			//费用计算，这里是整个vm一直保持alive的计算方式
			double newTotalUsedTime = Math.max(tmpList.get(tmpList.size() - 1).getFinishTime(), solution.getVMLeaseEndTime(vm))
						- Math.min(tmpList.get(0).getStartTime(), solution.getVMLeaseStartTime(vm));
			double extraCost = Math.ceil(newTotalUsedTime / VM.INTERVAL) * vm.getUnitCost() - solution.calcVMCost(vm); //oldVMTotalCost
			if(extraCost<minExtraCost){	
				minExtraCost = extraCost;
				bestList = tmpList;
			}
		}
		
		int selectedI = -1;
		for(int i = 0; i<VM.TYPE_NO; i++){		// try new VMs
			List<Allocation> tmpList = new ArrayList<Allocation>();
			boolean isSatisfied = true;
			for(int k = 0; k<PCP.size(); k++){
				Task task = PCP.get(k);
				double taskEST = task.getEST();	
				if(k > 0)
					taskEST = Math.max(taskEST, tmpList.get(k-1).getFinishTime());
				if(taskEST + task.getTaskSize() / VM.SPEEDS[i] > task.getLFT() + Evaluate.E){	//lft is not met
					isSatisfied = false;
					break;
				}
				tmpList.add(new Allocation(i, task, taskEST));
			}
			if(isSatisfied){
				double extraCost = Math.ceil((tmpList.get(tmpList.size() - 1).getFinishTime() - tmpList.get(0).getStartTime())/VM.INTERVAL)
						* VM.UNIT_COSTS[i];
				if(extraCost < minExtraCost){
					minExtraCost = extraCost;
					bestList = tmpList;
					selectedI = i;
				}
			}
		}
		if(selectedI != -1){
			VM vm = new VM(selectedI);
			for(Allocation e : bestList)
				e.setVM(vm);
		}
		if(bestList == null)		//fail to get a VM to support this PCP and thus fail to find a solution
			throw new RuntimeException();
		
		// schedule PCP on bestVM and set SS(task), AST(task)
		for(Allocation alloc : bestList){	
			alloc.setFinishTime(alloc.getStartTime() + alloc.getTask().getTaskSize()/alloc.getVM().getSpeed());
			
			Task task = alloc.getTask();
			task.setAssigned(true);		// set all tasks of P as assigned
			task.setAST(alloc.getStartTime());
			task.setAFT(alloc.getFinishTime());	
			solution.addTaskToVM(alloc.getVM(), alloc.getTask(), alloc.getStartTime(), true);
		}
		
		List<Allocation> allocList = solution.get(bestList.get(0).getVM());	//sort allocations on VM; it is necessary
		Collections.sort(allocList, new Comparator<Allocation>(){
			public int compare(Allocation o1, Allocation o2) {
				if(o1.getStartTime() > o2.getStartTime())
					return 1;
				else if(o1.getStartTime() < o2.getStartTime())
					return -1;
				return 0;
			}
		});	
	}
	
	//search a time slot in vm between EST and LFT for task allocation
	//returning -1 means this task can not be placed to this vm between EST and LFT, in the target solution
	private double searchStartTime(VM vm, Task task, double EST, double LFT){
		LinkedList<Allocation> list = solution.get(vm);
		
		for(int i = 0;i<list.size()+1;i++){
			double timeSlotStart, timeSlotEnd;
			if(i == 0){
				timeSlotStart = 0;
				timeSlotEnd	= list.get(i).getStartTime();
			}else if(i==list.size()){
				timeSlotStart = list.get(i-1).getFinishTime();
				timeSlotEnd = Double.MAX_VALUE;
			}else{
				timeSlotStart = list.get(i-1).getFinishTime();
				timeSlotEnd	= list.get(i).getStartTime();
			}
			double slackTime = LFT - EST - task.getTaskSize()/vm.getSpeed();
			if(EST + slackTime >= timeSlotStart){			//condition1：startTime satisfies
				double startTime = Math.max(timeSlotStart, EST);
				//condition2：slot is large enough to support this task
				if(timeSlotEnd - startTime >= task.getTaskSize() / vm.getSpeed())	
					return startTime;
			}
		}
		return -1;
	}
	private VM getEarliestVM(){
		VM ealiestVM = null;
		double earliestTime = Double.MAX_VALUE;
		for(VM vm : solution.keySet()){
			double startTime = solution.getVMLeaseStartTime(vm);
			if(startTime < earliestTime){
				earliestTime = startTime;
				ealiestVM = vm;
			}
		}
		return ealiestVM;
	}
	private VM getLatestVM(){
		VM latestVM = null;
		double latestTime = 0;
		for(VM vm : solution.keySet()){
			double finishTime = solution.getVMLeaseEndTime(vm);
			if(finishTime > latestTime){
				latestTime = finishTime;
				latestVM = vm;
			}
		}
		return latestVM;
	}
}