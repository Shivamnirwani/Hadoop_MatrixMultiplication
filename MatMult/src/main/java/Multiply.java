import org.apache.hadoop.mapreduce.*;
import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;


class Jodi implements WritableComparable<Jodi> {

	int i;
	int j;
	
	Jodi() {
		i = 0;
		j = 0;
	}
	
	Jodi(int i, int j) {
		this.i = i;
		this.j = j;
	}
	
	@Override
	public void readFields(DataInput inp) throws IOException {
		i = inp.readInt();
		j = inp.readInt();
	}

	@Override
	public void write(DataOutput outp) throws IOException {
		outp.writeInt(i);
		outp.writeInt(j);
	}		
	@Override
	public int compareTo(Jodi sajha) {
		
		if (i > sajha.i) {
			return 1;
		} else if ( i < sajha.i) {
			return -1;
		} else {
			if(j > sajha.j) {
				return 1;
			} else if (j < sajha.j) {
				return -1;
			}
		}
		return 0;
	}
	
	public String toString() {
		return i + " " + j + " ";
	}
}
class Symbol implements Writable {
	int tags;
	int indexes;
	double values;
	
	Symbol() {
		tags = 0;
		indexes = 0;
		values = 0.0;
	}
	
	Symbol(int tags, int indexes, double values) {
		this.tags = tags;
		this.indexes = indexes;
		this.values = values;
	}
	
	@Override
	public void readFields(DataInput inp) throws IOException {
		tags = inp.readInt();
		indexes = inp.readInt();
		values = inp.readDouble();
	}
	
	@Override
	public void write(DataOutput outp) throws IOException {
		outp.writeInt(tags);
		outp.writeInt(indexes);
		outp.writeDouble(values);
	}
}

public class Multiply {
	
	public static class MatrixMMapper extends Mapper<Object,Text,IntWritable,Symbol> {
		
		@Override
		public void map(Object keys, Text values, Context contexts)
				throws IOException, InterruptedException {
			String lineRead = values.toString();
			String[] tokensofStrings = lineRead.split(",");
			
			int indexes = Integer.parseInt(tokensofStrings[0]);
			double valuesSanket = Double.parseDouble(tokensofStrings[2]);

			Symbol e = new Symbol(0, indexes, valuesSanket);

			IntWritable valueKeys = new IntWritable(Integer.parseInt(tokensofStrings[1]));
			contexts.write(valueKeys, e);
		}
	}

	public static class MatrixNMapper extends Mapper<Object,Text,IntWritable,Symbol> {
		
		@Override
		public void map(Object keys, Text values, Context contexts)
				throws IOException, InterruptedException {
			String lineRead = values.toString();
			String[] tokensofStrings = lineRead.split(",");
			
			int indexes = Integer.parseInt(tokensofStrings[1]);
			double valuesSanket = Double.parseDouble(tokensofStrings[2]);

			Symbol e = new Symbol(1,indexes, valuesSanket);
			
			IntWritable valueKeys = new IntWritable(Integer.parseInt(tokensofStrings[0]));
			contexts.write(valueKeys, e);
		}
	}

	public static class MNReducer extends Reducer<IntWritable,Symbol, Jodi, DoubleWritable> {
	
	@Override
	public void reduce(IntWritable keys, Iterable<Symbol> value, Context contexts) 
			throws IOException, InterruptedException {
		
		ArrayList<Symbol> M = new ArrayList<Symbol>();
		ArrayList<Symbol> N = new ArrayList<Symbol>();
		
		Configuration conf = contexts.getConfiguration();
		
		for(Symbol sanket : value) {
			
			Symbol sanketTemp = ReflectionUtils.newInstance(Symbol.class, conf);
			ReflectionUtils.copy(conf, sanket, sanketTemp);
			
			if (sanketTemp.tags == 0) {
				M.add(sanketTemp);
			} else if(sanketTemp.tags == 1) {
				N.add(sanketTemp);
			}
		}
		
		for(int i=0;i<M.size();i++) {
			for(int j=0;j<N.size();j++) {
				
				Jodi p = new Jodi(M.get(i).indexes,N.get(j).indexes);
				double outputMultiply = M.get(i).values * N.get(j).values;

				contexts.write(p, new DoubleWritable(outputMultiply));
				}
			}
		}
	}
	
	public static class MNMap extends Mapper<Object, Text, Jodi, DoubleWritable> {
		@Override
		public void map(Object keys, Text values, Context contexts) 
				throws IOException, InterruptedException {

			String lineRead = values.toString();
			String[] valuesPairs = lineRead.split(" ");
			
			Jodi p = new Jodi(Integer.parseInt(valuesPairs[0]),Integer.parseInt(valuesPairs[1]));
			DoubleWritable finalValue = new DoubleWritable(Double.parseDouble(valuesPairs[2]));

			contexts.write(p, finalValue);
		}
	}
	
	public static class MNReduce extends Reducer<Jodi, DoubleWritable, Jodi, DoubleWritable> {
		@Override
		public void reduce(Jodi keys, Iterable<DoubleWritable> values, Context contexts)
		throws IOException, InterruptedException {
			
			double jod = 0.0;
			for(DoubleWritable value : values) {
				jod += value.get();
			}

			contexts.write(keys, new DoubleWritable(jod));
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		Job jobek = Job.getInstance();
		
		jobek.setJarByClass(Multiply.class);
		MultipleInputs.addInputPath(jobek, new Path(args[0]), TextInputFormat.class, MatrixMMapper.class);
		MultipleInputs.addInputPath(jobek, new Path(args[1]), TextInputFormat.class, MatrixNMapper.class);
		jobek.setReducerClass(MNReducer.class);
		
		jobek.setMapOutputValueClass(Symbol.class);
		jobek.setMapOutputKeyClass(IntWritable.class);
		jobek.setOutputKeyClass(Jodi.class);
		jobek.setOutputValueClass(DoubleWritable.class);
		
		jobek.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(jobek, new Path(args[2]));
		jobek.waitForCompletion(true);
		Job jobdo = Job.getInstance();
		
		jobdo.setJarByClass(Multiply.class);
		jobdo.setMapperClass(MNMap.class);
		jobdo.setReducerClass(MNReduce.class);

		jobdo.setMapOutputValueClass(DoubleWritable.class);
		jobdo.setMapOutputKeyClass(Jodi.class);
		jobdo.setOutputKeyClass(Jodi.class);
		jobdo.setInputFormatClass(TextInputFormat.class);
		
		jobdo.setOutputValueClass(DoubleWritable.class);

		jobdo.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(jobdo, new Path(args[2]));
		
		FileOutputFormat.setOutputPath(jobdo, new Path(args[3]));
		jobdo.waitForCompletion(true);
	}
}