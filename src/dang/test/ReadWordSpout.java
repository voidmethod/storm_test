package dang.test;



import java.io.BufferedReader;

import java.io.File;

import java.io.FileReader;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Random;

import java.util.List;

import java.util.Map;

import org.apache.commons.logging.Log;

import org.apache.commons.logging.LogFactory;



import backtype.storm.spout.SpoutOutputCollector;

import backtype.storm.task.TopologyContext;

import backtype.storm.testing.TestWordSpout;

import backtype.storm.topology.OutputFieldsDeclarer;

import backtype.storm.topology.base.BaseRichSpout;

import backtype.storm.tuple.Fields;

import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;



public class ReadWordSpout extends BaseRichSpout {

	public static Log LOG = LogFactory.getLog(TestWordSpout.class);

    private boolean _isDistributed;

    private SpoutOutputCollector _collector;

   // private BufferedReader br;

    private List<FileReader> fileReader;

    private File[]  _file; 

    private String _path;

    private List<String> pathList;

    public ReadWordSpout() {

        this(true);

    }

    

    public ReadWordSpout(String path) {

       // this(true);

        _path =path;

    }

    public ReadWordSpout(boolean isDistributed) {

        _isDistributed = isDistributed;

    }

        

    @Override

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

        _collector = collector;

       

        /**

         * create file

         */

        getFile();

    }

    

    @Override

	public void close() {

    	_file=null;

    	fileReader.clear();

    	fileReader= null;

    }

        

    @Override

	public void nextTuple() {

    	LOG.info("--------------nextTuple begin----------------");

        if(_isDistributed){

        	LOG.info("--------------_isDistributed---------------");

        		try {

					Thread.sleep(1000);

				} catch (InterruptedException e) {

					// TODO Auto-generated catch block

					e.printStackTrace();

				}

       

        	return;

        }

        //5s
        Utils.sleep(5000);
        //读取文件
        getFile();
        
        String word ="";

        for(int i=0; i <fileReader.size(); i++){

    		//System.out.println("------------------"+ i);

        BufferedReader br = new BufferedReader (fileReader.get(i));

				try {

					while ((word = br.readLine())!=null) {

						LOG.info("--------------word----------------"+word);

						try {
						  _collector.emit(new Values(word),word);

						Thread.sleep(100);

					} catch (InterruptedException e) {

						// TODO Auto-generated catch block

						e.printStackTrace();

						}

					  }

				} catch (Exception e) {

					// TODO Auto-generated catch block

					throw new RuntimeException("Error",e);

				}finally

				{

					_isDistributed= false;  //  _isDistributed= true;

				}

				try {

					br.close();

				} catch (IOException e) {

					// TODO Auto-generated catch block

					e.printStackTrace();

				}

//				deletefile(pathList.get(i));  //删除文件

    	}

        fileReader.clear();

        

        LOG.info("--------------nextTuple end----------------");

      

    }

    

    @Override

	public void ack(Object msgId) {



    }



    @Override

	public void fail(Object msgId) {

        

    }

    

    @Override

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("word"));

    }

/**

 * create file 

 */

    private void getFile(){

    	File file = new File(_path);

    	

    	fileReader = new ArrayList<FileReader>();

    	_file = file.listFiles();

    	//pathList = new ArrayList<String>();

    	LOG.info(_file.toString()+"--------------_file.toString()----------------");

    	file = null;

    	for(File tfile: _file){

    		LOG.info(tfile+"--------------tfile----------------");

    		try {

    			//pathList.add(tfile.getPath());

				fileReader.add(new FileReader (tfile));

				LOG.info(tfile+"--------------tfile----------------");

			} catch (Exception e) {

				// TODO Auto-generated catch block

				e.printStackTrace();

			}

    	}

    	

    	//fileReader.add(FileReader())

    	//fileReader = new FileReader();

    }

   private void deletefile(String path){

	File _file = new File(path);

	if(_file.exists()){

		_file.delete();

		_file = null;

	}

   }

}

