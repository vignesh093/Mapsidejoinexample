import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapsideMapper extends Mapper<LongWritable,Text,Text,Text>{
	enum MY_TRACK_COUNTER{FILE_AVAI_COUNTER,RECORD_COUNT,FILE_NOT_FOUND,SOME_OTHER_ERROR};
	public static HashMap<String,String> mymap=new HashMap<String	,String>();
	public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
	{
		String checkval=null;
		String temp=value.toString();
		String[] temp1=temp.split(",");
		checkval=mymap.get(temp1[2]);
		if(checkval!=null)
		{
			context.write(new Text(checkval),value);
		}
	}
	
		@Override
		protected void setup(Context context) throws IOException,InterruptedException
		{
			Path[] filelist=DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for(Path findlist:filelist)
			{
				if(findlist.getName().toString().trim().equals("mapmainfile.dat"))
				{
					context.getCounter(MY_TRACK_COUNTER.FILE_AVAI_COUNTER).increment(1);
					fetchvalue(findlist,context);
				}
			}
			
		}
		public void fetchvalue(Path realfile,Context context) throws NumberFormatException, IOException
		{
			BufferedReader buff=new BufferedReader(new FileReader(realfile.toString()));
			String s1;
			try
			{
			while((s1=buff.readLine())!=null)
			{
			String[] s2=s1.split(",");
			context.getCounter(MY_TRACK_COUNTER.RECORD_COUNT).increment(1);
			mymap.put(s2[0],s2[1]);
			}
			}
			catch(FileNotFoundException e)
			{
				context.getCounter(MY_TRACK_COUNTER.FILE_NOT_FOUND).increment(1);
				e.printStackTrace();
			}
			catch(IOException e)
			{
				context.getCounter(MY_TRACK_COUNTER.SOME_OTHER_ERROR).increment(1);
				e.printStackTrace();
			}
		}
}
