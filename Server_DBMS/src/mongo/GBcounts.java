package mongo;

public class GBcounts {

	private String column;
	private double sum = 0.0;
	private double min = Double.MAX_VALUE;
	private double max = Double.MIN_VALUE;
	private int count = 0;
	
	public GBcounts(String column) {
		this.column = column;
	}

	public String getColumn() {
		return column;
	}

	public void setColumn(String column) {
		this.column = column;
	}

	public double getSum() {
		return sum;
	}

	public void setSum(double sum) {
		this.sum = sum;
	}

	public double getMin() {
		return min;
	}

	public void setMin(double min) {
		this.min = min;
	}

	public double getMax() {
		return max;
	}

	public void setMax(double max) {
		this.max = max;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}
	
	public void add() {
		this.count++;
	}
	
	public void sumAdd(double value) {
		sum += value;
	}
	
	public void mmChange(double value) {
		if(this.min > value) {
			this.min = value;
		}
		if(this.max < value) {
			this.max = value;
		}
	}
}
