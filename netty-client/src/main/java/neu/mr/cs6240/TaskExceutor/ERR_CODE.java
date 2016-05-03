package neu.mr.cs6240.TaskExceutor;

/**
 * Error codes sent to server
 * 
 * @author smitha
 *
 */
public enum ERR_CODE {

		FILES_READ_SUCESSFULLY_FROM_S3(0), FILES_NOT_DOWNLOADED_S3(1), INVALID_COMMAND_FROM_SERVER(-100);

	private int code;

	private ERR_CODE(int value) {
		this.code = value;
	}

	public int val() {
		return code;
	}
}
