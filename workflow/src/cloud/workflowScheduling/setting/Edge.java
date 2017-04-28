package cloud.workflowScheduling.setting;

import java.util.*;

public class Edge {

	private Task source;
	private Task destination;
	private long dataSize;    

	public Edge(Task source, Task destination) {
		this.source = source;
		this.destination = destination;
	}
	
	//-------------------------------------getters && setters--------------------------------
	public long getDataSize() {
		return dataSize;
	}
	public Task getSource() {
		return source;
	}
	public Task getDestination() {
		return destination;
	}
	void setDataSize(long size) {
		this.dataSize = size;
	}

	//-------------------------------------overrides--------------------------------
	public String toString() {
		String s = "Edge [source=" + source + ", destination=" + destination
		 	+ ", size=" + dataSize + "]";
		return s;
	}
	
	//-------------------------------------comparator--------------------------------
	//used by Workflow
	static class EComparator implements Comparator<Edge>{
		boolean isDestination;	// if true, compare destinations; otherwise, compare sources
		List<Task> topoSort;
		public EComparator(boolean isDestination, List<Task> topoSort){	
			this.isDestination = isDestination;
			this.topoSort = topoSort;
		}
		public int compare(Edge o1, Edge o2) {
			Task task1 = isDestination ? o1.getDestination() : o1.getSource();
			Task task2 = isDestination ? o2.getDestination() : o2.getSource();
			int index1 = topoSort.indexOf(task1);
			int index2 = topoSort.indexOf(task2); 
			if(index1 > index2)
				return 1;
			else if(index1 < index2)
				return -1;
			return 0;
		}
	};
}