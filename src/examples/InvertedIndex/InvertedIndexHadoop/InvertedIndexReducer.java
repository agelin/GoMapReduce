import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text>{
        private Text fileNameList = new Text();
        protected void reduce(Text key, java.lang.Iterable<Text> values, Context context) throws java.io.IOException ,InterruptedException 
        {
                String fileList = "";
                Map<String, Integer> filemap = new HashMap<String, Integer>();
                for (Text text : values) {
                        //fileList += text.toString()+",";
                        if(filemap.containsKey(text.toString()))
                        {
                        	filemap.put(text.toString(), filemap.get(text.toString())+1);
                        }
                        else
                        	filemap.put(text.toString(), 1);
                }
                for (Entry e: filemap.entrySet())
                {
                	//System.out.println(e);
                	fileList = fileList + " " + e.getKey().toString() + "-"+e.getValue().toString();
                }
                //fileList = fileList.substring(0, fileList.length()-1);
                //fileNameList.set(fileList);
                //context.write(key, fileNameList);
                fileNameList.set(fileList);
                context.write(key, fileNameList);
        }

}