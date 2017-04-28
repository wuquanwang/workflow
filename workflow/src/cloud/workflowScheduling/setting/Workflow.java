package cloud.workflowScheduling.setting;

import static java.lang.Math.*;

import java.util.*;
import javax.xml.parsers.*;
import org.xml.sax.*;
import org.xml.sax.helpers.*;

//adjacent list to store workflow graph; 
//two dummy tasks entry and exit are  at the head and the end of arraylist, respectively
public class Workflow extends ArrayList<Task>{
	
	private static final long serialVersionUID = 1L;
	private double deadline = Double.MAX_VALUE;
	private int maxParallel;
	
	//only used in reading DAX
	private HashMap<String, TransferData> transferData = new HashMap<String, TransferData>(); //前提： fileName必须可作为标示
	private HashMap<String, Task> nameTaskMapping = new HashMap<String, Task>();
	
	public Workflow(String file) {
		super();
		Task.resetInternalId();	
		try {		//readDAX
			SAXParser sp = SAXParserFactory.newInstance().newSAXParser();
			sp.parse(new InputSource(file), new MyDAXReader());
			System.out.println("succeed to read DAX data from " + file);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		//-----------add tasks to this workflow: start----------------------
		for(Task t: nameTaskMapping.values())
			this.add(t);
		Task tentry = new Task(("entry"), 0);	
		Task texit = new Task(("exit"), 0);
		for(Task t: this){						//add edges to entry and exit
			if(t.getInEdges().size()==0){
				Edge e = new Edge(tentry, t);
				t.getInEdges().add(e);
				tentry.getOutEdges().add(e);
			}
			if(t.getOutEdges().size()==0){
				Edge e = new Edge(t, texit);
				t.getOutEdges().add(e);
				texit.getInEdges().add(e);
			}
		}
		this.add(0, tentry);					//add the entry and exit nodes to the workflows
		this.add(texit);
		//-----------add tasks to this workflow: end----------------------
		
		bind();
		topoSort();		// turn to a topological sort
		calcTaskLevels();
	}
	
	// Bind data flow to control flow
	private void bind(){
		Task tentry = this.get(0);
		Task texit = this.get(this.size() - 1);

		for(TransferData td : transferData.values()){	//Bind data flow to control flow
			Task source = td.getSource();
			List<Task> destinations = td.getDestinations();
			if(source == null){
				source = tentry;
				td.setSize(0);		//a setting: transfer time of input data is omitted --- setting to 0
			}
			if(destinations == null || destinations.size()==0)	
				destinations.add(texit);
			for(Task destination : destinations){
				boolean flag = true;
				for(Edge outEdge : source.getOutEdges()){
					if(outEdge.getDestination() == destination){
						outEdge.setDataSize(td.getSize());			//bind here
						flag = false;
					}
				}
				//an annoying problem in some DAX files: a data flow cannot be bound to existing control flows
				//flag to indicate whether this problem exists
				if(flag == true){
					Edge e = new Edge(source, destination);
					e.setDataSize(td.getSize());
					source.insertOutEdge(e);
					destination.insertInEdge(e);
					System.out.println("**************add a control flow*******************source: "
							+e.getSource().getName()+"; destination: "+e.getDestination().getName());
				}
			}
		}
	}
	
	// convert the task list of workflow to a topological sort based on Kahn algorithm; 
	// besides, calculate maximal parallel number and sort edges for each task
	private void topoSort(){
		// Empty list that will contain the sorted elements
		List<Task> topoList = new ArrayList<Task>();	
		//S←Set of all nodes with no incoming edges
		PriorityQueue<Task> S = new PriorityQueue<Task>(10, new Task.ParallelComparator());		
		S.add(this.get(0));		

		for(Task task : this)	//set topoCount to 0
			task.setTopoCount(0);
		
		this.maxParallel = -1;
		while(S.size()>0){
			maxParallel = Math.max(maxParallel, S.size());
			Task task = S.poll();				// remove a node n from S
			topoList.add(task);			// add n to tail of L
			for(Edge e : task.getOutEdges()){	// for each node m with an edge e from n to m do
				Task t = e.getDestination();
				t.setTopoCount(t.getTopoCount()+1);	//remove edge e from the graph--achieved by setting TopoCount here
				if(t.getTopoCount() == t.getInEdges().size())	//if m has no other incoming edges then
					S.add(t);					// insert m into S			
			}
		}
		// It is a low bound and a larger one may exists
		System.out.println("An approximate value for maximum parallel number: " + maxParallel);  
		
		Edge.EComparator ecForDestination = new Edge.EComparator(true, topoList);//sort edges for each task
		Edge.EComparator ecForSource = new Edge.EComparator(false, topoList);
		for(Task t : this){
			Collections.sort(t.getInEdges(), ecForSource);
			Collections.sort(t.getOutEdges(), ecForDestination);
		}
		
		Collections.copy(this, topoList);
	}
	
	//calculate heuristic information of tasks, e.g., bLvel, tLevel 
	private void calcTaskLevels(){
		double speed = VM.SPEEDS[VM.FASTEST];
		
		for(int j= this.size()-1; j>=0; j--){
			double bLevel = 0;	
			double sLevel = 0;
			Task task = this.get(j);
			for(Edge outEdge : task.getOutEdges()){
				Task child = outEdge.getDestination();
				bLevel = Math.max(bLevel, child.getbLevel() + outEdge.getDataSize() / VM.NETWORK_SPEED);
				sLevel = Math.max(sLevel, child.getsLevel());
			}
			task.setbLevel(bLevel + task.getTaskSize() / speed);
			task.setsLevel(sLevel + task.getTaskSize() / speed);
		}
		
		for(int j= this.size()-1; j>=0; j--){
			Task task = this.get(j);
			double ALAP = this.get(0).getbLevel();		//CPLength
			for(Edge outEdge : task.getOutEdges()){
				Task child = outEdge.getDestination();
				ALAP = Math.min(ALAP, child.getALAP()-outEdge.getDataSize() / VM.NETWORK_SPEED);
			}
			task.setALAP(ALAP - task.getTaskSize() / speed);
		}
		
		for(Task task : this){
			double arrivalTime = 0;
			for(Edge inEdge : task.getInEdges()){
				Task parent = inEdge.getSource();
				arrivalTime = Math.max(arrivalTime, 
						parent.gettLevel() + parent.getTaskSize() / speed + inEdge.getDataSize() / VM.NETWORK_SPEED);
			}
			task.settLevel(arrivalTime);
		}
		
//		Collections.sort(topoList, new Task.TLevelComparator());
//		System.out.println("topological sort and tlevel：");
//		for(Task t : topoList)
//			System.out.println(t.getName() +"\t"+t.gettLevel());
		Collections.sort(this, new Task.BLevelComparator());
		Collections.reverse(this);
		System.out.println("topological sort and blevel：");
		for(Task t : this)
			System.out.println(t.getName() +"\t"+t.getbLevel());
	}
	
	//called by ProLiS and LACO
	public void calcPURank(double theta){
		double speed = VM.SPEEDS[VM.FASTEST];
		for(int j= this.size()-1; j>=0; j--){
			double pURank = 0;	
			Task task = this.get(j);
			for(Edge outEdge : task.getOutEdges()){
				Task child = outEdge.getDestination();
				
				int flag = 1;
				if(theta != Double.MAX_VALUE){		// if theta = Double.MAX_VALUE, flag = 1
					double et = child.getTaskSize() / speed;
					double tt = outEdge.getDataSize() / VM.NETWORK_SPEED;
					double d = 1-Math.pow(theta, -et / tt);	//网络传输时间越大，d取值越接近于1
					if(d<random())
						flag = 0;
				}
				
				pURank = Math.max(pURank, child.getpURank() + flag * outEdge.getDataSize() / VM.NETWORK_SPEED);
			}
			task.setpURank(pURank + task.getTaskSize() / speed);
		}
//		Collections.sort(topoList, new Task.PURankComparator());
//		System.out.println("Topological sort and pURank：");
//		for(Task t : topoList)
//			System.out.println(t.getName() +"\t"+t.getpURank());
	}
	
	//--------------------------getters&setters--------------------------------------------
	public double getDeadline(){
		return deadline;
	}
	public void setDeadline(double deadline){
		this.deadline = deadline;
	}
	public int getMaxParallel() {
		return maxParallel;
	}

	//--------------------------private classes--------------------------------------------
	private class MyDAXReader extends DefaultHandler{
		private Stack<String> tags = new Stack<String>();
		private String childId;
		private Task lastTask;
		public void startElement(String uri, String localName, String qName, Attributes attrs) {
			if(qName.equals("job")){
				String id = attrs.getValue("id");
				if(nameTaskMapping.containsKey(id))		//id conflicts
					throw new RuntimeException();
				Task t = new Task(id, Double.parseDouble(attrs.getValue("runtime")));
				nameTaskMapping.put(id, t);
				lastTask = t;
			}else if(qName.equals("uses") && tags.peek().equals("job")){
				//After reading the element "job", the element "uses" means a trasferData (i.e., data flow)
				String filename =attrs.getValue("file");
					
				long fileSize = Long.parseLong(attrs.getValue("size"));
				TransferData td = transferData.get(filename);
				if(td == null){
					td = new TransferData(filename, fileSize);
				}
				if(attrs.getValue("link").equals("input")){
					td.addDestination(lastTask);
				}else{									//output
					td.setSource(lastTask);
				}
				transferData.put(filename, td);
			}else if(qName.equals("child") ){		
				childId = attrs.getValue("ref");
			}else if(qName.equals("parent") ){ 
				//After reading the element "child", the element "parent" means an edge (i.e., control flow)
				Task child = nameTaskMapping.get(childId);
				Task parent = nameTaskMapping.get(attrs.getValue("ref"));
				
				Edge e = new Edge(parent, child);			//control flow
				parent.insertOutEdge(e);
				child.insertInEdge(e);
			}
			tags.push(qName);
		}
		public void endElement(String uri, String localName,String qName) {
			tags.pop();
		}
	}
	
	private class TransferData{		//this class is only used in parsing DAX data
		private String name;
		private long size;
		private Task source;		//used to bind control flow and data flow
		private List<Task> destinations = new ArrayList<Task>();

		public TransferData(String name, long size) {
			this.name = name;
			this.size = size;
		}
		
		//-------------------------------------getters & setter--------------------------------
		public long getSize() {return size;}
		public Task getSource() {return source;}
		public void setSource(Task source) {this.source = source;}
		public void addDestination(Task t){destinations.add(t);}
		public List<Task> getDestinations() {return destinations;}
		public void setSize(long size) {
			this.size = size;
		}
		//-------------------------------------overrides--------------------------------
		public String toString() {return "TransferData [name=" + name + ", size=" + size + "]";}
	}
}