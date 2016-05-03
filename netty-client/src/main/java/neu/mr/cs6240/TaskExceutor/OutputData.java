package neu.mr.cs6240.TaskExceutor;

import java.io.Serializable;

/**
 * The Data Structure after reading the climate files
 *
 * @author smitha
 * @author prasad
 */
public class OutputData implements Serializable {

	public Double getDryBulbTemp() {
		return dryBulbTemp;
	}

	public void setDryBulbTemp(Double dryBulbTemp) {
		this.dryBulbTemp = dryBulbTemp;
	}

	public OutputData(Integer wban, Integer date, Integer time, Double dryBulbTemp) {
		super();
		this.wban = wban;
		this.date = date;
		this.time = time;
		this.dryBulbTemp = dryBulbTemp;
	}

	Integer wban;
	Integer date;
	Integer time;

	public Integer getWban() {
		return wban;
	}

	public void setWban(Integer wban) {
		this.wban = wban;
	}

	public Integer getDate() {
		return date;
	}

	public void setDate(Integer date) {
		this.date = date;
	}

	public Integer getTime() {
		return time;
	}

	public void setTime(Integer time) {
		this.time = time;
	}

	Double dryBulbTemp;

	@Override
	public String toString() {
		return "OutputData [wban=" + wban + ", date=" + date + ", time=" + time + ", dryBulbTemp=" + dryBulbTemp + "]";
	}

}
