package neu.mr.cs6240.TaskExceutor;

/**
 * Common configuration parameters
 * 
 * @author smitha
 *
 */
class Configurations {

	public final static int TIME_OUT_SECS_MAP_TASK_TO_FAIL = 10 * 60;
	public final static int NUM_OF_THREADS_SPAWN_MAP_TASK = 5;
	public final static int SPILL_AFTER_MB = 128 * 1024 * 1024;
}
