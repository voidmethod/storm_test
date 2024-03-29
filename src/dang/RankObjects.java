package dang;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.json.simple.JSONValue;
 
 
@SuppressWarnings("serial")
public class RankObjects implements IBasicBolt {
 
    @SuppressWarnings("rawtypes")
    List<List> _rankings = new ArrayList<List>();
 
    int _count;
    Long _lastTime = null;
 
    public RankObjects(int n) {
        _count = n;
    }
 
 
    @SuppressWarnings("rawtypes")
    private int _compare(List one, List two) {
 
        long valueOne = (Long) one.get(1);
        long valueTwo = (Long) two.get(1);
 
        long delta = valueTwo - valueOne;
        if(delta > 0) {
            return 1;
        } else if (delta < 0) {
            return -1;
        } else {
            return 0;
        }
 
    } //end compare
 
    private Integer _find(Object tag) {
        for(int i = 0; i < _rankings.size(); ++i) {
 
            Object cur = _rankings.get(i).get(0);
            if (cur.equals(tag)) {
                return i;
            }
 
        }
 
        return null;
 
    }
 
    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context) {
 
    }
 
    @SuppressWarnings("rawtypes")
    public void execute(Tuple tuple, BasicOutputCollector collector) {
 
        Object tag = tuple.getValue(0);
 
 
        Integer existingIndex = _find(tag);
        if (null != existingIndex) {
            _rankings.set(existingIndex, tuple.getValues());
        } else {
 
            _rankings.add(tuple.getValues());
 
 
        }
 
 
        Collections.sort(_rankings, new Comparator<List>() {
            public int compare(List o1, List o2) {
                return _compare(o1, o2);
            }
        });
 
 
        if (_rankings.size() > _count) {
            _rankings.remove(_count);
        }
 
        long currentTime = System.currentTimeMillis();
        if(_lastTime==null || currentTime >= _lastTime + 2000) {
            collector.emit(new Values(JSONValue.toJSONString(_rankings)));
            _lastTime = currentTime;
        }
    }
 
    public void cleanup() {
    }
 
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("list"));
    }


	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
 
}