package cloud.workflowScheduling.methods;

import cloud.workflowScheduling.setting.*;

public interface Scheduler {
	Solution schedule(Workflow wf);
}
