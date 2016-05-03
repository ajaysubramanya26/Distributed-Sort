package neu.mr.cs6240.utils;

/**
 * This class holds the info for the state of a client
 * @author prasadmemane
 *
 */
public class ClientState {

	private String msg;
	private String result;

	public ClientState(String msg, String result) {
		this.msg = msg;
		this.result = result;
	}

	public String getMsg() {
		return msg;
	}

	public void setMsg(String msg) {
		this.msg = msg;
	}

	public String getResult() {
		return result;
	}

	public void setResult(String result) {
		this.result = result;
	}

	@Override
	public String toString() {
		return "ClientState [msg=" + msg + ", result=" + result + "]";
	}

}
