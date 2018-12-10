import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.io.IntWritable;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class Cleaning_data
{
        public static int a = 0;

        public static class CleanMap extends Mapper<Object,Text,Text,Text>
        {
                private Text clean = new Text();
                public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
                        String NoPI;
                        String NoPK;
                        String NoPeI;
                        String NoPeK;
                        String NoCI;
                        String NoCK;
                        String NoMI;
                        String NoMK;
                        String VTyCd;
                        String line = value.toString();

                        String[] linepart = line.split(",");
                        if (!linepart[1].equals("TIME")){
                        if (linepart.length > 25){
                        System.out.println("hello");
                        String Date = linepart[0];
                        String Borough = linepart[2];
                        String Zip = linepart[3];
                        if (linepart[6].length() != 0){
                        NoPI = linepart[11];
                        NoPK = linepart[12];
                        NoPeI = linepart[13];
                        NoPeK = linepart[14];
                        NoCI = linepart[15];
                        NoCK = linepart[16];
                        NoMI = linepart[17];
                        NoMK = linepart[18];
                        VTyCd = linepart[25];}
                        else{
                        NoPI = linepart[10];
                        NoPK = linepart[11];
			NoPeI = linepart[12];
                        NoPeK = linepart[13];
                        NoCI = linepart[14];
                        NoCK = linepart[15];
                        NoMI = linepart[16];
                        NoMK = linepart[17];
                        VTyCd = linepart[24];}
                        if (Borough.length() != 0 && Date.length() != 0 && Zip.length() != 0 && NoPI.length() != 0 && NoPK.length() != 0 && NoPeI.length() != 0 && NoPeK.length() != 0 && NoCI.length() != 0 && NoCK.length() != 0
                            && NoMI.length() != 0 && NoMK.length() != 0 && VTyCd.length() != 0  ){
                        context.write(new Text(""), new Text(Date+","+Borough+","+Zip+","+NoPI+","+NoPK+","+NoPeI+","+NoPeK+","+NoCI+","+NoCK+","+NoMI+","+NoMK+","+VTyCd));
                        }}}}
        }

        public static class CleanMap2 extends Mapper<Object,Text,Text,IntWritable>
        {
                public void map(Object key,Text value,Context context)
                throws IOException, InterruptedException {
                        String line = value.toString();
                        String[] linepart = line.split(",");
                        String n_year = linepart[0].trim();
                        String new_year = "Year1-"+":"+n_year.substring(n_year.length() - 4);
                        Text year = new Text(new_year);
                        String new_year2 = "Year2-"+":"+n_year.substring(n_year.length() - 4);
                        Text year2 = new Text(new_year2);
                        String new_year3 = "Year3-"+":"+n_year.substring(n_year.length() - 4);
                        Text year3 = new Text(new_year3);
                        String new_year4 = "Year4-"+":"+n_year.substring(n_year.length() - 4);
                        Text year4 = new Text(new_year4);
                        String new_d = linepart[0].trim();
                        String new_date = "Date-"+":"+new_d;
                        Text date = new Text(new_date);
                        String new_borough = linepart[1].trim();
                        new_borough = "Borough-"+":"+new_borough;
                        Text borough = new Text(new_borough);
                        String new_zip = linepart[2].trim();
                        new_zip = "Zip-"+":"+new_zip;
                        Text zip = new Text(new_zip);
                        String new_vety = linepart[11].trim();
                        new_vety = "Vehicle_Ty-"+":"+new_vety;
                        Text vtycd = new Text(new_vety);
                        int num = Integer.valueOf(linepart[5]);
                        int val_acc = 1;
             		context.write(date, new IntWritable(val_acc));
                        context.write(vtycd, new IntWritable(val_acc));

                        int val_fat = Integer.valueOf(linepart[4]) + Integer.valueOf(linepart[6]) + Integer.valueOf(linepart[8]) + Integer.valueOf(linepart[10]);
                        if (val_fat > 0)
                        {
                                context.write(borough, new IntWritable(val_fat));
                                context.write(zip, new IntWritable(val_fat));
			}

                        int val_year1 = (Integer.valueOf(linepart[3]) + Integer.valueOf(linepart[5]));
                        if (val_year1 > 0)
                        {
                                context.write(year, new IntWritable(val_year1));
                        }

                        int val_year2 = (Integer.valueOf(linepart[4]) + Integer.valueOf(linepart[6]));
                        if (val_year2 > 0)
                        {
                                context.write(year2, new IntWritable(val_year2));
                        }
                        if (linepart[7] != "0" && linepart[8] != "0"){
                        int val_year3 = (Integer.valueOf(linepart[7]) + Integer.valueOf(linepart[8]));
                        context.write(year3, new IntWritable(val_year3));}

                        if (linepart[9] != "0" && linepart[10] != "0"){
                        int val_year4 = (Integer.valueOf(linepart[9]) + Integer.valueOf(linepart[10]));
                        context.write(year4, new IntWritable(val_year4));}
        }}

        public static class Clean_Reducer extends Reducer<Text,IntWritable,Text,IntWritable>
        {
                private Map<Text, IntWritable> countMap = new HashMap<>();
                int max = 0;
                public void reduce(Text key, Iterable<IntWritable> val, Context context)
                throws IOException, InterruptedException {
                        int sum = 0 ;
                        for(IntWritable value : val)
                        {
                                sum = sum + value.get();
                        }
                        countMap.put(new Text(key),new IntWritable(sum));
                }

                protected void cleanup(Context context) throws IOException, InterruptedException{
                        Map<Text, IntWritable> sortedMap = countMap;
                        int counter = 0;
                        int max_d = 0;
                        int max_b = 0;
                        int max_z = 0;
                        int max_v = 0;
                        int max_y1 = 0;
                        int max_y2 = 0;
                        int max_y3 = 0;
                        int max_y4 = 0;
                        Text d = new Text();
                        Text b = new Text();
                        Text z = new Text();
                        Text v = new Text();
                        Text y1 = new Text();
                        Text y2 = new Text();
                        Text y3 = new Text();
                        Text y4 = new Text();
                        IntWritable dat = new IntWritable();
                        IntWritable bor = new IntWritable();
                        IntWritable zip = new IntWritable();
                        IntWritable vety = new IntWritable();
                        IntWritable year1 = new IntWritable();
                        IntWritable year2 = new IntWritable();
                        IntWritable year3 = new IntWritable();
			IntWritable year4 = new IntWritable();
                        for (Text k: sortedMap.keySet() )
                        {
                                String str = k.toString();
                                str = str.substring(0,5);
                                if (str.equals("Date-")){
                                dat = sortedMap.get(k);
                                if (dat.get() > max_d)
                                {
                                        max_d = dat.get();
                                        d = k;
                                }}
                                else if (str.equals("Borou"))
                                {
                                        bor = sortedMap.get(k);
                                        if (bor.get() > max_b)
                                        {
                                                max_b = bor.get();
                                                b = k;
                                        }
                                }
                                else if (str.equals("Zip-:"))
                                {
                                        zip = sortedMap.get(k);
                                        if (zip.get() > max_z)
                                        {
                                                max_z = zip.get();
                                                z = k;
                                        }

                                }
                                else if (str.equals("Vehic"))
                                {
                                        vety = sortedMap.get(k);
                                        if (vety.get() > max_v)
                                        {
                                                max_v = vety.get();
                                                v = k;
                                        }

                                }
                                else if (str.equals("Year1"))
                                {
                                        year1 = sortedMap.get(k);
                                        if ( year1.get() > max_y1)
                                        {
                                                max_y1 = year1.get();
                                                y1 = k;
                                        }
                                }
                                else if (str.equals("Year2"))
                                {
                                        year2 = sortedMap.get(k);
                                        if ( year2.get() > max_y2)
                                        {
                                                max_y2 = year2.get();
                                                y2 = k;
                                        }
                                }
                                else if (str.equals("Year3"))
				{
                                        year3 = sortedMap.get(k);
                                        if ( year3.get() > max_y3)
                                        {
                                                max_y3 = year3.get();
                                                y3 = k;
                                        }
                                }
                                else if (str.equals("Year4"))
                                {
                                        year4 = sortedMap.get(k);
                                        if ( year4.get() > max_y4)
                                        {
                                                max_y4 = year4.get();
                                                y4 = k;
                                        }
                                }
                        }
                        context.write(d, new IntWritable(max_d));
                        context.write(b, new IntWritable(max_b));
                        context.write(z, new IntWritable(max_z));
                        context.write(v, new IntWritable(max_v));
                        context.write(y1, new IntWritable(max_y1));
                        context.write(y2, new IntWritable(max_y2));
                        context.write(y3, new IntWritable(max_y3));
                        context.write(y4, new IntWritable(max_y4));
                }
        }


public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Clean data");
    Configuration mapAConf = new Configuration(false);
    ChainMapper.addMapper(job, CleanMap.class, Object.class, Text.class, Text.class, Text.class,mapAConf);
    Configuration mapBConf = new Configuration(false);
    ChainMapper.addMapper(job, CleanMap2.class, Text.class, Text.class, Text.class, IntWritable.class,mapBConf);
    job.setJarByClass(Cleaning_data.class);
    job.setReducerClass(Clean_Reducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}




