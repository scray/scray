package scray.sync.api;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class QuerySpec {
  public static class Condition {
    public String field;
    public final String op; // only "=="
    public String value;



    public String getField() {
		return field;
	}



	public void setField(String field) {
		this.field = field;
	}



	public String getValue() {
		return value;
	}



	public void setValue(String value) {
		this.value = value;
	}



	public Condition(String field, String op, String value) {
      this.field = field; this.op = op; this.value = value;
    }
  }
  private final Map<String, Condition> conditions = new ConcurrentHashMap<>();
  public  Map<String, Condition> conditions() { return conditions; }

  public QuerySpec add(String field, String op, String value) {
    conditions.put(field,  new Condition(field, op, value));
    return this;
  }
}
