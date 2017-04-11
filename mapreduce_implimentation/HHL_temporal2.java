package com.cloudyhadoop.hadoop.mapreduce;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.Scanner;
import java.util.Stack;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;



public class HHL_temporal2 {


	public static long MAX_TIMESTAMP = Long.MAX_VALUE - 3;
	// ttr tt[]=new ttr[2];
	public static int N;
	public int M;
	public int init;
	static long lcnt;
	long new_cnt;
	static long tmin;// the smallest timestamp and largest timestamp in the
	static long tmax;
					// graph
	public static int vertexid2level[];// obtain the level order from the vertex id;
	public static int leveltovertexid[];// obtain the vertex id from the level order;
	int in_deg[], out_deg[], p[], tb[], q[], fq[], tq[];
	boolean compressed_or_not;
	double ordering_time;
	double indexing_time;
	int stamps;
	Vector<Vector<tri>> fa,ch;
	// vector<vector<pair<int,int>>>cv;
	Vector<Integer>chv;
	static Vector2 links0=new Vector2();
	static Vector2 links1=new Vector2();
	//iVector<iVector<EdgeQuadruple>> links0 = new iVector<iVector<EdgeQuadruple>>();
	//iVector<iVector<EdgeQuadruple>> links1 = new iVector<iVector<EdgeQuadruple>>();
	
	//iVector<iVector<Integer>> static_edges0 = new iVector<iVector<Integer>>();
	//iVector<iVector<Integer>> static_edges1 = new iVector<iVector<Integer>>();
	
	static Vector3 label0=new Vector3();
	static Vector3 label1=new Vector3();
	//iVector<iVector<iVector<LabelQunituple>>> label0 = new iVector<iVector<iVector<LabelQunituple>>>();
	//iVector<iVector<iVector<LabelQunituple>>> label1 = new iVector<iVector<iVector<LabelQunituple>>>();
	//iVector<iVector<iVector<LabelQunituple>>> sorted_links0 = new iVector<iVector<iVector<LabelQunituple>>>();
	//iVector<iVector<iVector<LabelQunituple>>> sorted_links1 = new iVector<iVector<iVector<LabelQunituple>>>();
	// iHeap<long> pq[2];//û�õ�
	// iHeap<long> opq;//û�õ�
	 iMap mark= new iMap();
	 iMap busmark= new iMap();
	 iMap fathermark= new iMap();
	// map<pair<int,int>,node>dep;
	// vector<pair<int,int> >id2v;
	Vector<EdgeQuintuple> query_sets;

	
	
	public HHL_temporal2(){
		
	}

	
	void addInitialIndex()
	{
		for(int i=0; i<N; i++){
			
            int v = vertexid2level[i];
            
            for(int k=0; k<links0.get(v).m_num; k++)
            {
            	int start_time = links0.get(v).get(k).ta;
            	int same_timestamp_vertex = links0.get(v).get(k).v;
				if(leveltovertexid[same_timestamp_vertex]<=i)continue;
				int startBus=links0.get(v).get(k).busid;
                //long ts = links0.get(v).get(k).tb;
                LabelQunituple p=new LabelQunituple(leveltovertexid[v], start_time, start_time, -1, -2, -1,startBus);
                add_Label( label0.get(leveltovertexid[v]), p );
            }
		}
		
		for(int i=0; i<N; i++){
			
            int v = vertexid2level[i];
            
            for(int k=0; k<links1.get(v).m_num; k++)
            {
            	int start_time = links1.get(v).get(k).ta;
            	int same_timestamp_vertex = links1.get(v).get(k).v;
				if(leveltovertexid[same_timestamp_vertex]<=i)continue;
				int startBus=links1.get(v).get(k).busid;
                //long ts = links0.get(v).get(k).tb;
                LabelQunituple p=new LabelQunituple(leveltovertexid[v], start_time, start_time, -1, -2, -1,startBus);
                add_Label( label1.get(leveltovertexid[v]), p );
            }
		}
		
		int x;
		x=0;
	}
	
	 void load_temporal_graph(String graph_name) {
		// clock_t load_graph_start = clock();
		// File file = new File(graph_name);
		try {
			
			Scanner scanner = new Scanner(new File(graph_name));
			// scanner.useDelimiter(" |:|\\r\\n"");
			// String ret_val = scanner.next();
			N = scanner.nextInt();
			System.out.println(N + " nodes");

			links0.re_allocate(N);
			links0.m_num = N;
			links1.re_allocate(N);
			links1.m_num = N;
			label0.re_allocate(N);
			label0.m_num = N;
			label1.re_allocate(N);
			label1.m_num = N;
//			static_edges0.re_allocate(N);
//			static_edges0.m_num = N;
//			static_edges1.re_allocate(N);
//			static_edges1.m_num = N;
			// pq[0].initialize(N);
			// pq[1].initialize(N);
			// opq.initialize(N);
			mark.initialize(N);
			busmark.initialize(N);
			fathermark.initialize(N);
			vertexid2level = new int[N];
			leveltovertexid = new int[N];
			// memset(in_deg, 0x00, 4*N);
			// memset(out_deg, 0x00, sizeof(int)*N);
			tmin = Long.MAX_VALUE-3;
			tmax = -1;
			int x = 0, y = 0, w = 0, lines = 0;
			int t = 0;
			int busid = 0;
			int num_edges = 0;
			while (scanner.hasNext()) {
				x = scanner.nextInt()-1;
				y=scanner.nextInt()-1;
				busid=scanner.nextInt();
				while (scanner.hasNext()) {
					t = scanner.nextInt();
					if (t == -1)
						break;
					w = scanner.nextInt();
					if (t < tmin)
						tmin = t;
					if (w + t > tmax)
						tmax = t;
					EdgeQuadruple forwrad_triple = new EdgeQuadruple(y, t, w + t, busid);
					EdgeQuadruple backward_triple = new EdgeQuadruple(x, t, w + t, busid);
					if (x == y) {
						continue;
					}
					num_edges++;
					// static_edges0[x].sorted_insert(y);
					// static_edges1[y].sorted_insert(x);
					links0.get(x).sorted_insert(forwrad_triple,1);
					links1.get(y).sorted_insert(backward_triple,2);// EdgeQuadruple��������ض���<tb��С����
			
				}
			}
			
//			BufferedWriter bw = new BufferedWriter(new FileWriter("D:\\gz\\v2e.txt"));
//			for(int i=0; i<N; i++)
//			{
//				for(int j=links0.get(i).m_num-1;j>=0;j--)
//				{
//					bw.write(Integer.toString(i));
//					bw.write(" ");
//					bw.write(Integer.toString(j));///���źͱ���Ķ�Ӧ��ϵ������level�ͱ���Ķ�Ӧ��ϵ����ʹ��level����Ҫ��
//					bw.write("\n");
//				}
//			}
//			bw.flush();
//			bw.close();
//			BufferedWriter bw = new BufferedWriter(new FileWriter("D:\\gz\\austinGraphJava.txt"));
//			bw.write(Integer.toString(N));
//			bw.write("\n");
//			for(int i=0; i<N; i++){
//				//fprintf(index_file, "%d:", i);
//				bw.write(Integer.toString(i));
//				bw.write(":");
//				for(int j=0; j<links0.get(i).m_num; j++)
//				{
//						String v=Integer.toString(links0.get(i).get(j).v);
//						String ta=Integer.toString((int) links0.get(i).get(j).ta);
//						String tb=Integer.toString((int) (links0.get(i).get(j).tb));
//						String Busid=Integer.toString(links0.get(i).get(j).busid);
//						String s=v+" "+ta+" "+tb+" "+Busid+" ";
//						bw.write(s);
//				}
//				bw.write("-1 ");
//				for(int j=0; j<links1.get(i).m_num; j++)
//					
//					{
//					String v=Integer.toString(links1.get(i).get(j).v);
//					String ta=Integer.toString((int) links1.get(i).get(j).ta);
//					String tb=Integer.toString((int) (links1.get(i).get(j).tb));
//					String Busid=Integer.toString(links1.get(i).get(j).busid);
//					String s=v+" "+ta+" "+tb+" "+Busid+" ";
//					bw.write(s);
//					}
//				bw.write( "-2\n");
//			}
//			bw.flush();
//			bw.close();
//			System.out.println("output graph complete");
			num_edges = 0;
			for (int i = 0; i < links0.m_num; i++) {
				num_edges += links0.get(i).m_num;
			}
			System.out.println(num_edges + " edges");
			scanner.close();
			// clock_t load_graph_stop = clock();
			// printf( "Load Graph Time:
			// %0.2lf\n",(double)(load_graph_stop-load_graph_start)/CLOCKS_PER_SEC
			// );

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	 public static void load_temporal_graph_for_austin(Vector2 link0,Vector2 link1){

			
			//link=new Vector2();
			try {
				//String fileUri = "hdfs://192.168.112.128:8020/user/ningzhq/example";
				String fileUri = "hdfs://192.168.112.128:8020/user/ningzhq/austin/austin_data";
				Configuration conf = new Configuration();
				//conf.set("fs.defaultFS", "hdfs://192.168.112.128:8020");
				FileSystem fs = FileSystem.get(new URI(fileUri), conf);
				
				FSDataInputStream inputStream = fs.open(new Path("hdfs://192.168.112.128:8020/user/ningzhq/austin/austin_data/austinJava.txt"));	
				//FSDataInputStream inputStream = fs.open(new Path("hdfs://192.168.112.128:8020/user/ningzhq/example/example_data/exampleJava.txt"));	
				
				Scanner scanner = new Scanner(inputStream);
				// scanner.useDelimiter(" |:|\\r\\n"");
				// String ret_val = scanner.next();
				N = scanner.nextInt();
				System.out.println(N + " nodes");
				
				link0.re_allocate(N);
				link0.m_num = N;
				link1.re_allocate(N);
				link1.m_num = N;
				int x = 0, y = 0, w = 0;
				int t = 0;
				int busid = 0;
				int num_edges = 0;
				while (scanner.hasNext()) {
					x = scanner.nextInt();
					while (scanner.hasNext()) {
						y = scanner.nextInt();
						if (y == -1)
							break;
						t = scanner.nextInt();
						w = scanner.nextInt();
						busid = scanner.nextInt();
						if (t < tmin)
							tmin = t;
						if (w + t > tmax)
							tmax = t;
						EdgeQuadruple forwrad_triple = new EdgeQuadruple(y, t, w + t, busid);
						EdgeQuadruple backward_triple = new EdgeQuadruple(x, t, w + t, busid);
						if (x == y) {
							continue;
						}
						num_edges++;
						// static_edges0[x].sorted_insert(y);
						// static_edges1[y].sorted_insert(x);
						link0.get(x).push_back(forwrad_triple);
						link1.get(y).sorted_insert(backward_triple,2);// EdgeQuadruple的运算符重定义<tb从小到大
					}
				}
				num_edges = 0;
				for (int i = 0; i < link0.m_num; i++) {
					num_edges += link0.get(i).m_num;
				}
				System.out.println(num_edges + " edges");
				scanner.close();
				// clock_t load_graph_stop = clock();
				// printf( "Load Graph Time:
				// %0.2lf\n",(double)(load_graph_stop-load_graph_start)/CLOCKS_PER_SEC
				// );

			} catch (Exception e) {
				e.printStackTrace();
			}

			//return link;
		
	 }
	 
	 
	 public static void load_temporal_graph_for_map12(Vector2 link0,Vector2 link1) {
			
			//link=new Vector2();
			try {
				//String fileUri = "hdfs://192.168.112.128:8020/user/ningzhq/example";
				String fileUri = "hdfs://192.168.112.128:8020/user/ningzhq/gzdata";
				Configuration conf = new Configuration();
				//conf.set("fs.defaultFS", "hdfs://192.168.112.128:8020");
				FileSystem fs = FileSystem.get(new URI(fileUri), conf);
				
				FSDataInputStream inputStream = fs.open(new Path("hdfs://192.168.112.128:8020/user/ningzhq/gzdata/gz.txt"));	
				//FSDataInputStream inputStream = fs.open(new Path("hdfs://192.168.112.128:8020/user/ningzhq/example/example_data/exampleJava.txt"));	
				
				Scanner scanner = new Scanner(inputStream);
				// scanner.useDelimiter(" |:|\\r\\n"");
				// String ret_val = scanner.next();
				N = scanner.nextInt();
				System.out.println(N + " nodes");
				
				link0.re_allocate(N);
				link0.m_num = N;
				link1.re_allocate(N);
				link1.m_num = N;
				int x = 0, y = 0, w = 0;
				int t = 0;
				int busid = 0;
				int num_edges = 0;
				while (scanner.hasNext()) {
					x = scanner.nextInt()-1;
					y=scanner.nextInt()-1;
					
					busid=scanner.nextInt();
					while (scanner.hasNext()) {
						t = scanner.nextInt();
						if (t == -1)
							break;
						w = scanner.nextInt();
						if (t < tmin)
							tmin = t;
						if (w + t > tmax)
							tmax = t;
						EdgeQuadruple forwrad_triple = new EdgeQuadruple(y, t, w + t, busid);
						EdgeQuadruple backward_triple = new EdgeQuadruple(x, t, w + t, busid);
						if (x == y) {
							continue;
						}
						//num_edges++;
						// static_edges0[x].sorted_insert(y);
						// static_edges1[y].sorted_insert(x);
						link0.get(x).sorted_insert(forwrad_triple,1);
						link1.get(y).sorted_insert(backward_triple,2);
					}
				}
				num_edges = 0;
				for (int i = 0; i < link0.m_num; i++) {
					num_edges += link0.get(i).m_num;
				}
				System.out.println(num_edges + " edges");
				scanner.close();
				// clock_t load_graph_stop = clock();
				// printf( "Load Graph Time:
				// %0.2lf\n",(double)(load_graph_stop-load_graph_start)/CLOCKS_PER_SEC
				// );

			} catch (Exception e) {
				e.printStackTrace();
			}

			//return link;
		}
	 
	 
	 
	
	public static Vector2 load_temporal_graph_for_map(String graph_name) {
		
		Vector2 link=new Vector2();
		try {
			
			String fileUri = "hdfs://192.168.112.128:8020/user/ningzhq/gz";
			Configuration conf = new Configuration();
			//conf.set("fs.defaultFS", "hdfs://192.168.112.128:8020");
			FileSystem fs = FileSystem.get(new URI(fileUri), conf);
			
			FSDataInputStream inputStream = fs.open(new Path("hdfs://192.168.112.128:8020/user/ningzhq/gz/gz.txt"));	
			
			Scanner scanner = new Scanner(inputStream);
			// scanner.useDelimiter(" |:|\\r\\n"");
			// String ret_val = scanner.next();
			N = scanner.nextInt();
			System.out.println(N + " nodes");
			
			link.re_allocate(N);
			link.m_num = N;
			int x = 0, y = 0, w = 0;
			int t = 0;
			int busid = 0;
			int num_edges = 0;
			while (scanner.hasNext()) {
				x = scanner.nextInt()-1;
				y=scanner.nextInt()-1;
				busid=scanner.nextInt();
				while (scanner.hasNext()) {
					t = scanner.nextInt();
					if (t == -1)
						break;
					w = scanner.nextInt();
					if (t < tmin)
						tmin = t;
					if (w + t > tmax)
						tmax = t;
					EdgeQuadruple forwrad_triple = new EdgeQuadruple(y, t, w + t, busid);
					//EdgeQuadruple backward_triple = new EdgeQuadruple(x, t, w + t, busid);
					if (x == y) {
						continue;
					}
					num_edges++;
					// static_edges0[x].sorted_insert(y);
					// static_edges1[y].sorted_insert(x);
					link.get(x).sorted_insert(forwrad_triple,1);
					
				}
			}
			num_edges = 0;
			for (int i = 0; i < link.m_num; i++) {
				num_edges += link.get(i).m_num;
			}
			System.out.println(num_edges + " edges");
			scanner.close();
			// clock_t load_graph_stop = clock();
			// printf( "Load Graph Time:
			// %0.2lf\n",(double)(load_graph_stop-load_graph_start)/CLOCKS_PER_SEC
			// );

		} catch (Exception e) {
			e.printStackTrace();
		}

		return link;
	}
	
public static Vector2 load_temporal_graph_for_map2(String graph_name) {
	
	Vector2 link=new Vector2();
	try {
		
		String fileUri = "hdfs://192.168.112.128:8020/user/ningzhq/";
		Configuration conf = new Configuration();
		//conf.set("fs.defaultFS", "hdfs://192.168.112.128:8020");
		FileSystem fs = FileSystem.get(new URI(fileUri), conf);
		
		FSDataInputStream inputStream = fs.open(new Path("hdfs://192.168.112.128:8020/user/ningzhq/gzdata/gz.txt"));	
		
		Scanner scanner = new Scanner(inputStream);
		
		// scanner.useDelimiter(" |:|\\r\\n"");
		// String ret_val = scanner.next();
		N = scanner.nextInt();
		System.out.println(N + " nodes");
		
		link.re_allocate(N);
		link.m_num = N;
		int x = 0, y = 0, w = 0;
		int t = 0;
		int busid = 0;
		int num_edges = 0;
		while (scanner.hasNext()) {
			x = scanner.nextInt()-1;
			y=scanner.nextInt()-1;
			busid=scanner.nextInt();
			while (scanner.hasNext()) {
				t = scanner.nextInt();
				if (t == -1)
					break;
				w = scanner.nextInt();
				if (t < tmin)
					tmin = t;
				if (w + t > tmax)
					tmax = t;
				//EdgeQuadruple forwrad_triple = new EdgeQuadruple(y, t, w + t, busid);
				EdgeQuadruple backward_triple = new EdgeQuadruple(x, t, w + t, busid);
				if (x == y) {
					continue;
				}
				num_edges++;
				// static_edges0[x].sorted_insert(y);
				// static_edges1[y].sorted_insert(x);
				link.get(y).sorted_insert(backward_triple,2);
				
			}
		}
		num_edges = 0;
		for (int i = 0; i < link.m_num; i++) {
			num_edges += link.get(i).m_num;
		}
		System.out.println(num_edges + " edges");
		scanner.close();
		// clock_t load_graph_stop = clock();
		// printf( "Load Graph Time:
		// %0.2lf\n",(double)(load_graph_stop-load_graph_start)/CLOCKS_PER_SEC
		// );

	} catch (Exception e) {
		e.printStackTrace();
	}

	return link;
}
	

static int[]  load_order_file_for_austin(String file_name,int n){
    System.out.println(file_name);
    int map_vertexid2level[]=new int[n];
    try {
		
		Scanner scanner = new Scanner(new File(file_name));
		int idx=0;
		while (scanner.hasNext()) {
			map_vertexid2level[idx] = scanner.nextInt();
			idx++;
		}
		scanner.close();
    }catch(Exception e){
    	e.printStackTrace();
    }
    return map_vertexid2level;
}
static void  load_order_file(String file_name){
        System.out.println(file_name);
        try {
			
			Scanner scanner = new Scanner(new File(file_name));
			int idx=0;
			while (scanner.hasNext()) {
				vertexid2level[idx] = scanner.nextInt();
				idx++;
			}
			scanner.close();
        }catch(Exception e){
        	e.printStackTrace();
        }
    }
	
	public static void load_order_file_for_map(String file_name,int vertexid2level[]){
        System.out.println(file_name);
        try {
			
			Scanner scanner = new Scanner(new File(file_name));
			int idx=0;
			while (scanner.hasNext()) {
				vertexid2level[idx] = scanner.nextInt();
				idx++;
			}
			scanner.close();
        }catch(Exception e){
        	e.printStackTrace();
        }
    }
	
	
	
	 void load_query_set(String query_file_name){
		 System.out.println(query_file_name);
		 try {
			 int u,v;
		     long ta, tb;
		     query_sets=new Vector<EdgeQuintuple>();
		     Scanner scanner = new Scanner(new File(query_file_name));
		     while(scanner.hasNext()){
		    	 u=scanner.nextInt();
		    	 v=scanner.nextInt();
		    	 ta=scanner.nextInt();
		    	 tb=scanner.nextInt();
		    	 EdgeQuintuple eq =new EdgeQuintuple(u,v,ta,tb, -1);
		    	 query_sets.add(eq);
		     }
		     scanner.close();

	        }catch(Exception e){
	        	e.printStackTrace();
	        }
		 System.out.println("Num of querys:"+query_sets.size());
	    }
	
	
	static void func(){
    	for(int i=0; i<N; i++){
    	            leveltovertexid[vertexid2level[i]] = i;
    	        }
    }
	
	public static void func_for_map(int leveltovertexid[],int vertexid2level[] ){
    	for(int i=0; i<N; i++){
    	            leveltovertexid[vertexid2level[i]] = i;
    	        }
    }
	
	 static void add_Label(Vector22  insert_list, LabelQunituple new_label){
	        int last_list_index = insert_list.m_num -1;
	        if(last_list_index>=0 && insert_list.get(last_list_index).get(0).v == new_label.v){
	            insert_list.get(last_list_index).sorted_insert(new_label);
	        }else{
	            VectorLabelQunituple new_list=new VectorLabelQunituple();
	            new_list.sorted_insert(new_label);
	            insert_list.push_back(new_list);
	        }
	    }
	 static void add_Label_for_map(Vector22Writable  insert_list, LabelQunituple new_label){
	        int last_list_index = insert_list.m_num -1;
	        if(last_list_index>=0 && insert_list.get(last_list_index).get(0).v == new_label.v){
	            insert_list.get(last_list_index).sorted_insert(new_label);
	        }else{
	            VectorLabelQunituple new_list=new VectorLabelQunituple();
	            new_list.sorted_insert(new_label);
	            insert_list.push_back(new_list);
	        }
	    }
	 
	 static int Search_Label_Qunituple(long timestamp, VectorLabelQunituple label_list){
	        if(label_list.m_num <50){
	            int w =0; 
	            for(; w<label_list.m_num; w++){
	                if(label_list.get(w).ta < timestamp) continue;
	                break;
	            }
	            return w;
	        }else{
	            int x,y;
	            int start_pos=0;
	            for(x=0, y=label_list.m_num-1; x<=y;){
	                int mid = x+ (y-x)/2;//do not use (x+y)/2 in case of overflow 
	                if(label_list.get(mid).ta == timestamp){
	                    start_pos = mid;
	                    y = mid-1;
	                }
	                if ( label_list.get(mid).ta < timestamp) x = mid+1;
	                else y = mid-1;
	            }
	            return start_pos;
	        }
	    }
	 
	    static boolean reachable_query(int src, int tar, long timerange_a, long timerange_b){
	        src = leveltovertexid[src];
	        tar = leveltovertexid[tar];
	        int x,y;

	        for(int i=0, j=0; i<label0.get(src).m_num && j<label1.get(tar).m_num;){
	            VectorLabelQunituple outlabel_list = label0.get(src).get(i);
	            x = outlabel_list.get(0).v;
	            VectorLabelQunituple inlabel_list = label1.get(tar).get(j);
	            y = inlabel_list.get(0).v;
	            
	            if( x < y) i++;
	            else if( x > y) j++;
	            else{
	                long outlabel_ta, outlabel_tb;
	                long inlabel_ta, inlabel_tb;
	                //int k=0, m=0;
	                int k = Search_Label_Qunituple(timerange_a, outlabel_list);
	                int m = Search_Label_Qunituple(timerange_a, inlabel_list);
	                finished:
	                for(; k<outlabel_list.m_num && m< inlabel_list.m_num;){
	                    outlabel_ta = outlabel_list.get(k).ta;
	                    outlabel_tb = outlabel_list.get(k).tb;
	                    inlabel_ta = inlabel_list.get(m).ta;
	                    inlabel_tb = inlabel_list.get(m).tb;
	                    if(outlabel_ta < timerange_a){k++; continue;}
	                    if(inlabel_ta < timerange_a){ m++; continue;}
	                    if(inlabel_tb > timerange_b){
	                        //i++; j++;
	                        break finished;
	                        //break;
	                    }
	                    if(outlabel_tb <= inlabel_ta ){
	                        return true;
	                    }else m++;
	                }
	                
	                i++;
	                j++;
	                
	            }
	        }
	        return false;
	    }
	
	    
	    static boolean reachable_query_for_map(int src, int tar, long timerange_a, long timerange_b,Vector3_for_map label0,Vector3_for_map label1){
	        
	        int x,y;

	        for(int i=0, j=0; i<label0.get(src).m_num && j<label1.get(tar).m_num;){
	            VectorLabelQunituple outlabel_list = label0.get(src).get(i);
	            x = outlabel_list.get(0).v;
	            VectorLabelQunituple inlabel_list = label1.get(tar).get(j);
	            y = inlabel_list.get(0).v;
	            
	            if( x < y) i++;
	            else if( x > y) j++;
	            else{
	                long outlabel_ta, outlabel_tb;
	                long inlabel_ta, inlabel_tb;
	                //int k=0, m=0;
	                int k = Search_Label_Qunituple(timerange_a, outlabel_list);
	                int m = Search_Label_Qunituple(timerange_a, inlabel_list);
	                finished:
	                for(; k<outlabel_list.m_num && m< inlabel_list.m_num;){
	                    outlabel_ta = outlabel_list.get(k).ta;
	                    outlabel_tb = outlabel_list.get(k).tb;
	                    inlabel_ta = inlabel_list.get(m).ta;
	                    inlabel_tb = inlabel_list.get(m).tb;
	                    if(outlabel_ta < timerange_a){k++; continue;}
	                    if(inlabel_ta < timerange_a){ m++; continue;}
	                    if(inlabel_tb > timerange_b){
	                        //i++; j++;
	                        break finished;
	                        //break;
	                    }
	                    if(outlabel_tb <= inlabel_ta ){
	                        return true;
	                    }else m++;
	                }
	                
	                i++;
	                j++;
	                
	            }
	        }
	        return false;
	    }
	    
	    
	    static int Search_Edge_Quadruple(long timestamp, VectorEdgeQuadruple edge_list, int search_case){
	        //search case 0 search based on EdgeQuadruple ta value;
	    	if(search_case ==0){
	            if(edge_list.m_num < 20){//����ĳ�������20����ֱ����
	                int w=0; 
	                for( ; w<edge_list.m_num; w++){
	                    if(edge_list.get(w).ta < timestamp) continue;
	                    break;
	                }
					return w;
	            }else{//���߶���20����������
	                int x,y;
	                int start_pos=0;
	                for(x=0, y=edge_list.m_num-1; x<=y;){
	                    int mid = x+ (y-x)/2;//do not use (x+y)/2 in case of overflow 

	                    if(edge_list.get(mid).ta == timestamp){
	                        start_pos = mid;
	                        y = mid-1;
	                    }
	                    if ( edge_list.get(mid).ta < timestamp) x = mid+1;
	                    else y = mid-1;
	                }
	                if(edge_list.get(start_pos).ta == timestamp)
	                	return start_pos;
	                else return y>0?y:0;
	            }
	        }else{
	            //search case 1 search based on EdgeQuadruple tb value;
	            if(edge_list.m_num < 20){
	                int w=edge_list.m_num>0? edge_list.m_num-1: -1; 
	                for( ; w>=0; w--){
	                    if(edge_list.get(w).tb > timestamp) continue;
	                    break;
	                }
	                return w;
	            }else{
	                //int x,y;
	                int end_pos= edge_list.m_num-1;
	                for(int x=0, y=edge_list.m_num-1; x<=y;){
	                    int mid= x+ (y-x)/2;//do not use (x+y)/2 in case of overflow 
	                    if(edge_list.get(mid).ta == timestamp){///ta->tb?
	                        end_pos = mid;
	                        x= mid+1;
	                    }
	                    if ( edge_list.get(mid).ta < timestamp) x = mid+1;///ta->tb?
	                    else y = mid-1;
	                }
	                return end_pos;
	            }
	        }
	    }
	    
	    public static  class MyMapper extends Mapper<LongWritable, Text, IntWritable, Text>
	    {

	    	
	    	//private final static IntWritable mapOutputValue=new IntWritable(1);
	    	//private Text mapOutputKey=new Text();
	    	Vector2 map_link;		    
	    	Vector3_for_map map_label;
	    	iMap mark;
	    	iMap busmark;
	   	 	iMap fathermark;
	    	int map_leveltovertexid[];
	    	int map_vertexid2level[];
	    	iHeap nodes_pq;
	    	int N;
	    	  @Override
	          protected void setup(Context context)
	                  throws IOException, InterruptedException {
	    		  Configuration conf = context.getConfiguration();
	    		  N = conf.getInt("nNodes", -1);
	    		  
	    		  map_link=HHL_temporal2.load_temporal_graph_for_map(null);
	    		  //map_label=HHL_temporal2.label1;
	    		 
	    		  map_label=new Vector3_for_map();
	    		  
	    		  map_label.re_allocate(N);
	    		  map_label.m_num = N;
	    		  map_leveltovertexid=new int[64];
	    		  map_vertexid2level=new int[64];
	    		  mark= new iMap();
	    		  busmark= new iMap();
	  	   	 	  fathermark= new iMap();
	    		  nodes_pq=new iHeap();
	    		  nodes_pq.initialize(N);
	    		  mark.initialize(N);
	  			  busmark.initialize(N);
	  			  fathermark.initialize(N);
	    		  for(int i=0;i<N;i++)
	    		  {
	    			  map_leveltovertexid[i]=i;
	    			  map_vertexid2level[i]=i;
	    		  }
	    		 // HHL_temporal2.load_order_file("/opt/TTL_source/gz.order");
	    		  //map_leveltovertexid=HHL_temporal2.leveltovertexid;
	    		 // map_vertexid2level=HHL_temporal2.vertexid2level;
	          }
	    	  @Override
	    	  protected void cleanup(Context context) throws IOException,
	    	  InterruptedException {
	    		  for(int i=0;i<map_label.m_num;i++)
		            	if(map_label.get(i).m_num>0)
		            	{
		            		String longStr="";
		            		for(int j=0;j<map_label.get(i).m_num;j++)
		            		{
		            			longStr=Integer.toString(map_label.get(i).get(j).m_num)+" ";
		            			for(int k=0;k<map_label.get(i).get(j).m_num;k++)
		            			{
		            				String v=Integer.toString(map_label.get(i).get(j).get(k).v);
		    						String ta=Integer.toString((int) map_label.get(i).get(j).get(k).ta);
		    						String tb=Integer.toString((int) (map_label.get(i).get(j).get(k).tb));
		    						String father=Integer.toString(map_label.get(i).get(j).get(k).father);
		    						String busid=Integer.toString(map_label.get(i).get(j).get(k).busid);
		    						String s=v+" "+ta+" "+tb+" "+father+" "+busid+" ";
		    						longStr+=s;
		            			}
		            		
				            	IntWritable iw=new IntWritable(i);
				            	longStr+="-1 ";
				            	Text valueStr=new Text(longStr);
				            	context.write(iw,valueStr);
		            		}
		            		
		            		
		            	}
	    	  }
	    	  
	    	  
	    	
	    	@Override
	    	protected void map(LongWritable key,  Text value, Context context)
			throws IOException, InterruptedException {
		
	    		int walk_time;
	    		String line=value.toString();
	    		//System.out.println("====="+line+"=====");
		
	    		int currI = -1;
	    		StringTokenizer stringTokenizer=new StringTokenizer(line);
	    		while(stringTokenizer.hasMoreTokens()){
	    			String v1=stringTokenizer.nextToken();
	    			//String e=stringTokenizer.nextToken();
	   
	    			int i=Integer.parseInt(v1);
	    			int k;
	    			
	    			currI=i;

	    			int w_[]=new int[N];
	    			int ii;
	    			
	    			
	    			
	    			mark.clean();
		            for( k=map_link.get(i).m_num-1; k>=0; k--){
		            	
		            	
		            	 // map_link=HHL_temporal2.load_temporal_graph_for_map("/opt/TTL_source/gz.txt");
		    	      //  System.out.println("==============="+map_link.m_num);
		    	      //  nodes_pq[1]=new iHeap();
		    	        
		    	      //  nodes_pq[1].initialize(N);
		    	        
		    	            int v = map_leveltovertexid[i];		         
		    	            //forward search
		    	            
		    	            if(map_leveltovertexid[map_link.get(v).get(k).v]<=i)
		    	            	continue;
		    	            nodes_pq.clean();
		    	            
		    	            for(ii=0;ii<N;ii++)
		    	            	w_[ii]=-1;
		    					//links[v][k]�е�k�Ƕ���v�ĵ�k�����ߣ���������ָ��Ķ���id��links[v][k].v
		    	                int start_time = map_link.get(v).get(k).ta;
		    	                int startBus=map_link.get(v).get(k).busid;///��ѯƥ��ʱlabel_in��Ҫ֪��·���ϵ�һ̨������
		    	                mark.insert(v, (int)start_time);
		    	              
		    	               /// for( ; k>=0 && start_time == map_link.get(v).get(k).ta ; k--){///����ʱ����ͬ�����Ų�ͬ������ȥ�����Գ���ʱ����ͬ�ı�
		    	                    int same_timestamp_vertex = map_link.get(v).get(k).v;
		    						if(map_leveltovertexid[same_timestamp_vertex]<=i)continue;
		    						int ts = map_link.get(v).get(k).tb;
		    	                    mark.insert(same_timestamp_vertex, (int) ts); 
		    	                    busmark.insert(same_timestamp_vertex, map_link.get(v).get(k).busid);
		    	                    fathermark.insert(same_timestamp_vertex, v);
		    	                    nodes_pq.insert(same_timestamp_vertex, ts-start_time,map_link.get(v).get(k).busid);
		    						w_[same_timestamp_vertex]=-2;
		    	              ///  }
		    	             ///  k++;
		    	                //LabelQunituple p=new LabelQunituple(map_leveltovertexid[v], start_time, start_time, -1, -2, -1,startBus);
		    	               /// add_Label( label0.get(map_leveltovertexid[v]), p );
		    	                //label[0][map_leveltovertexid[v]].sorted_insert(p);
		    	                while(!nodes_pq.empty()){
		    	                    int u = nodes_pq.head();
		    	                    int u_bus=nodes_pq.bus();
		    	                    nodes_pq.pop();
		    	                    long query_range_start = start_time, query_range_end;
		    	                    if(mark.exist(u)){
		    	                        query_range_end = mark.m_data[u];
		    	                    }else{
		    	                        query_range_end = Long.MAX_VALUE-3;
		    	                    }
		    	                  ///  boolean reachable = reachable_query(v, u, query_range_start, query_range_end);
		    	                   /// if(reachable){
		    	                      //  if(mark.exist(u))
		    	                      //  {   mark.erase(u);//ȥ����ʱ�����һ���·��
		    	                   //     continue;}
		    	                    ///}
		    	                    long arrival_u_time = query_range_end;
		    	                    for(int j=Search_Edge_Quadruple(arrival_u_time, map_link.get(u), 0); j < map_link.get(u).m_num; j++){
		    	                        // use binary search to get correct position
		    	                    	int busid = map_link.get(u).get(j).busid;
		    	                    	if(busid==u_bus)
		    	                    		walk_time=0;
		    	                    	else
		    	                    		walk_time=3;
		    	                        if(map_link.get(u).get(j).ta <arrival_u_time+walk_time) continue;
		    	                        int w = map_link.get(u).get(j).v;
		    							if(map_leveltovertexid[w]<=i)continue;
		    	                        long taa = map_link.get(u).get(j).ta;
		    	                        long tbb = map_link.get(u).get(j).tb;
		    	                        
		    	                        int father = u;
		    	                        /*if(mark.exist(w) &&  mark[w] < tbb) {
		    	                          continue;
		    	                          }*/
		    	                        long w_start_time = start_time, w_end_time;
		    	                        if( mark.exist(w)){
		    	                            w_end_time = mark.m_data[w];
		    	                        }else{
		    	                            w_end_time = Long.MAX_VALUE-3;
		    	                        }		                     
		    	                        if(tbb-start_time<w_end_time-w_start_time||tbb-start_time==w_end_time-w_start_time
		    								&&(w_[w]==-1||w_[father]>=0&&w_[father]<w_[w]||map_leveltovertexid[father]<w_[w]))
		    	                        {
		    	                            mark.insert(w,  (int) tbb);///!!!����������w�Ĺ淶·��������ʱ����ͬ��·�������ж�����w��μ�¼��mark��occur����,��ʹ����ʱ�䲻һ��Ҳ��¼������ʵ��Υ�������ĵ�֧������
		    	                            nodes_pq.insert(w, tbb - start_time,busid);
		    	                            if(busmark.m_data[father] == busid) 
		    	                            	busmark.insert(w, busid);
		    	                            else
		    	                            	busmark.insert(w, -1);
		    	                            		                          
		    	                            
//		    	                            if(busmark.m_data[father] == busid)
//		    	                            {
//		    	                            	busmark.insert(w, busid);
//		    	                            	walk_time=0;
//		    	                            }
//		    	                            else 
//		    	                            {
//		    	                            	busmark.insert(w, -1);
//		    	                            	walk_time=3;
//		    	                            }
		    	                          //  mark.insert(w,  (int) tbb+walk_time);
		    	                           // nodes_pq[0].insert(w, tbb - start_time+walk_time);
		    	                            ///
		    	                            
		    	                            fathermark.insert(w, father);
//		    								if(tbb-start_time==w_end_time-w_start_time)printf("%d",w_[w]);
		    								w_[w]=w_[father];
		    								if(w_[w]<0||map_leveltovertexid[father]<w_[w])
		    									w_[w]=map_leveltovertexid[father];
//		    								if(tbb-start_time==w_end_time-w_start_time)printf(" %d\n",w_[w]);
		    	                        }
		    	                    }
		    	                }

		    	                for( int counter =0; counter <mark.occur.m_num; counter++){
		    	                    if(mark.exist(mark.occur.get(counter))){
		    	                    	
		    	                        int j = mark.occur.get(counter);
		    	                        if(j==3)
		    	                    		{int x;
		    	                    		x= 44;
		    	                    		}
		    	                        
		    	                        if(j!=v){
		    	                        	int ta = start_time;
		    	                            int tb = mark.m_data[j];
		    	                            int father = (int) fathermark.m_data[j];
		    	                            int busid = (int) busmark.m_data[j];
		    	                            assert(father>=0);
		    	                            LabelQunituple new_p=new LabelQunituple(map_leveltovertexid[v],ta, tb, map_leveltovertexid[father], w_[j], busid,startBus); 
		    	                            //label[1][map_leveltovertexid[j]].sorted_insert(new_p);
		    	                            add_Label_for_map(map_label.get(map_leveltovertexid[j]), new_p); 
//		    								printf("%d in: %d %d %d %d %d\n",j,v,ta,tb,father,busid);
		    	                            //lcnt++;
		    	                        }
		    	                    }
		    	                }
		    	                mark.clean();
		    	                busmark.clean();
		    	                fathermark.clean();
		    	                nodes_pq.clean();
		    	            
    			
		    			//context.write(map_leveltovertexid, null);
		    	                
		            
		            }
	    			
	    			

	    			 	 
	    		}
	    		
	    		int xx=0;
	    		xx=currI;
//	    		for(int i=0;i<map_label.m_num;i++)
//	            	if(map_label.get(i).m_num>0)
//	            	{
//	            		String longStr="";
//	            		for(int j=0;j<map_label.get(i).m_num;j++)
//	            		{
//	            			longStr=Integer.toString(map_label.get(i).get(j).m_num)+" ";
//	            			for(int k=0;k<map_label.get(i).get(j).m_num;k++)
//	            			{
//	            				String v=Integer.toString(map_label.get(i).get(j).get(k).v);
//	    						String ta=Integer.toString((int) map_label.get(i).get(j).get(k).ta);
//	    						String tb=Integer.toString((int) (map_label.get(i).get(j).get(k).tb));
//	    						String father=Integer.toString(map_label.get(i).get(j).get(k).father);
//	    						String busid=Integer.toString(map_label.get(i).get(j).get(k).busid);
//	    						String s=v+" "+ta+" "+tb+" "+father+" "+busid+" ";
//	    						longStr+=s;
//	            			}
//	            		
//			            	IntWritable iw=new IntWritable(i);
//			            	longStr+="-1 ";
//			            	Text valueStr=new Text(longStr);
//			            	context.write(iw,valueStr);
//	            		}
//	            		
//	            		
//	            	}
	    	}
	    
	    }
	    public static  class CombineMapper2 extends Mapper<LongWritable, Text, IntWritable, Text>{

	    	
	    	//private final static IntWritable mapOutputValue=new IntWritable(1);
	    	//private Text mapOutputKey=new Text();
	    	Vector2 map_link0;	
	    	Vector2 map_link1;	
	    	Vector3_for_map map_label0;
	    	Vector3_for_map map_label1;
	    	iMap mark;
	    	iMap busmark;
	   	 	iMap fathermark;
	    	int map_leveltovertexid[];
	    	int map_vertexid2level[];
	    	iHeap nodes_pq0;
	    	iHeap nodes_pq1;
	    	int N;
	    	  @Override
	          protected void setup(Context context)
	                  throws IOException, InterruptedException {
	    		 // Configuration conf = context.getConfiguration();
	    		 // N = conf.getInt("nNodes", -1);
	    		  
	    		  map_link0=new Vector2();
	    		  map_link1=new Vector2();
	    		  HHL_temporal2.load_temporal_graph_for_austin(map_link0,map_link1);
	    		  //map_label=HHL_temporal2.label1;
	    		  N=map_link0.m_num;
	    		  map_label0=new Vector3_for_map();
	    		  
	    		  map_label0.re_allocate(N);
	    		  map_label0.m_num = N;
	    		  
	    		  map_label1=new Vector3_for_map();
	    		  
	    		  map_label1.re_allocate(N);
	    		  map_label1.m_num = N;
	    		  
	    		  map_leveltovertexid=new int[N];
	    		  map_vertexid2level=new int[N];
	    		  mark= new iMap();
	    		  busmark= new iMap();
	  	   	 	  fathermark= new iMap();
	    		  nodes_pq0=new iHeap();
	    		  nodes_pq0.initialize(N);
	    		  nodes_pq1=new iHeap();
	    		  nodes_pq1.initialize(N);
	    		  mark.initialize(N);
	  			  busmark.initialize(N);
	  			  fathermark.initialize(N);
//	  			  for(int i=0; i<N; i++){
//    	            map_leveltovertexid[i] = i;
//    	            map_vertexid2level[i]=i;
//    	         }
	  			  
	  			  
	    		  map_vertexid2level=HHL_temporal2.load_order_file_for_austin("/opt/TTL_source/austin.order",N);
	    		  for(int i=0; i<N; i++){
	    	            map_leveltovertexid[map_vertexid2level[i]] = i;
	    	        }
	    		  //map_leveltovertexid=HHL_temporal2.leveltovertexid;
	    		 // map_vertexid2level=HHL_temporal2.vertexid2level;
	          }
	    	  @Override
	    	  protected void cleanup(Context context) throws IOException,
	    	  InterruptedException {
	    		  for(int i=0;i<map_label0.m_num;i++)
		            	if(map_label0.get(i).m_num>0)
		            	{
		            		String longStr="";
		            		for(int j=0;j<map_label0.get(i).m_num;j++)
		            		{
		            			longStr+="-7 "+Integer.toString(map_label0.get(i).get(j).m_num)+" ";///-7 is for label_out
		            			for(int k=0;k<map_label0.get(i).get(j).m_num;k++)
		            			{
		            				String v=Integer.toString(map_label0.get(i).get(j).get(k).v);
		    						String ta=Integer.toString( map_label0.get(i).get(j).get(k).ta);
		    						String tb=Integer.toString( (map_label0.get(i).get(j).get(k).tb));
		    						String father=Integer.toString(map_label0.get(i).get(j).get(k).father);
		    						String ww=Integer.toString(map_label0.get(i).get(j).get(k).w);
		    						String busid=Integer.toString(map_label0.get(i).get(j).get(k).busid);
		    						String ebus=Integer.toString(map_label0.get(i).get(j).get(k).endOrStartBus);
		    						String s=v+" "+ta+" "+tb+" "+father+" "+ww+" "+busid+" "+ebus+" ";
		    						longStr+=s;
		            			}
		            		
				            	IntWritable iw=new IntWritable(i);
				            	//longStr+="-1 ";
				            	Text valueStr=new Text(longStr);
				            	context.write(iw,valueStr);
				            	longStr=null;
				            	longStr="";
		            		}
		            		
		            		
		            	}
	    		  
	    		  
	    		  
	    		  for(int i=0;i<map_label1.m_num;i++)
		            	if(map_label1.get(i).m_num>0)
		            	{
		            		String longStr="";
		            		for(int j=0;j<map_label1.get(i).m_num;j++)
		            		{
		            			longStr+="-8 "+Integer.toString(map_label1.get(i).get(j).m_num)+" ";///-8 is for label_in
		            			for(int k=0;k<map_label1.get(i).get(j).m_num;k++)
		            			{
		            				String v=Integer.toString(map_label1.get(i).get(j).get(k).v);
		    						String ta=Integer.toString( map_label1.get(i).get(j).get(k).ta);
		    						String tb=Integer.toString( (map_label1.get(i).get(j).get(k).tb));
		    						String father=Integer.toString(map_label1.get(i).get(j).get(k).father);
		    						String ww=Integer.toString(map_label1.get(i).get(j).get(k).w);
		    						String busid=Integer.toString(map_label1.get(i).get(j).get(k).busid);
		    						String ebus=Integer.toString(map_label1.get(i).get(j).get(k).endOrStartBus);
		    						String s=v+" "+ta+" "+tb+" "+father+" "+ww+" "+busid+" "+ebus+" ";
		    						longStr+=s;
		            			}
		            		
				            	IntWritable iw=new IntWritable(i);
				            	//longStr+="-1 ";
				            	Text valueStr=new Text(longStr);
				            	longStr=null;
				            	longStr="";
				            	context.write(iw,valueStr);			            	
		            		}
		            		
		            		
		            	}
	    	  }
	    	  
	    	  
	    	
	    	@Override
	    	protected void map(LongWritable key,  Text value, Context context)
			throws IOException, InterruptedException {
		
	    		int walk_time;
	    		String line=value.toString();
	    		//System.out.println("====="+line+"=====");
	    		
	    		StringTokenizer stringTokenizer=new StringTokenizer(line);
	    		while(stringTokenizer.hasMoreTokens()){
	    			String v1=stringTokenizer.nextToken();
	    			//String e=stringTokenizer.nextToken();
	   
	    			int i=Integer.parseInt(v1);
	    			int k;
	    
	    			//if(i>10)
	    				//continue;

	    			int w_[]=new int[N];
	    			int ii;
	    			
	    			

    	            int v = map_leveltovertexid[i];	
	    			mark.clean();
		            for( k=map_link0.get(v).m_num-1; k>=0; k--){	        	         
		    	            //forward search
		    	            
		    	            if(map_leveltovertexid[map_link0.get(v).get(k).v]<=i)
		    	            	continue;
		    	            nodes_pq0.clean();
		    	            
		    	            for(ii=0;ii<N;ii++)
		    	            	w_[ii]=-1;
		    					//links[v][k]�е�k�Ƕ���v�ĵ�k�����ߣ���������ָ��Ķ���id��links[v][k].v
		    	                int start_time = map_link0.get(v).get(k).ta;
		    	                int startBus=map_link0.get(v).get(k).busid;///��ѯƥ��ʱlabel_in��Ҫ֪��·���ϵ�һ̨������
		    	                mark.insert(v, (int)start_time);
		    	              
		    	                for( ; k>=0 && start_time == map_link0.get(v).get(k).ta ; k--){///����ʱ����ͬ�����Ų�ͬ������ȥ�����Գ���ʱ����ͬ�ı�
		    	                    int same_timestamp_vertex = map_link0.get(v).get(k).v;
		    						if(map_leveltovertexid[same_timestamp_vertex]<=i)continue;
		    	                    long ts = map_link0.get(v).get(k).tb;
		    	                    mark.insert(same_timestamp_vertex, (int) ts); 
		    	                    busmark.insert(same_timestamp_vertex, map_link0.get(v).get(k).busid);
		    	                    fathermark.insert(same_timestamp_vertex, v);
		    	                    nodes_pq0.insert(same_timestamp_vertex, ts-start_time,map_link0.get(v).get(k).busid);
		    						w_[same_timestamp_vertex]=-2;
		    	                }
		    	               k++;
		    	                LabelQunituple p=new LabelQunituple(map_leveltovertexid[v], start_time, start_time, -1, -2, -1,startBus);
		    	                add_Label_for_map( map_label0.get(map_leveltovertexid[v]), p );
		    	                //label[0][map_leveltovertexid[v]].sorted_insert(p);
		    	                while(!nodes_pq0.empty()){
		    	                    int u = nodes_pq0.head();
		    	                    int u_bus=nodes_pq0.bus();
		    	                    nodes_pq0.pop();
		    	                    long query_range_start = start_time, query_range_end;
		    	                    if(mark.exist(u)){
		    	                        query_range_end = mark.m_data[u];
		    	                    }else{
		    	                        query_range_end = Long.MAX_VALUE-3;
		    	                    }
		    	                    int src = map_leveltovertexid[v];
		    	        	        int tar = map_leveltovertexid[u];
		    	                   boolean reachable = reachable_query_for_map(src,tar, query_range_start, query_range_end,map_label0,map_label1);
		    	                   if(reachable){
		    	                      if(mark.exist(u))
		    	                        {   mark.erase(u);//ȥ����ʱ�����һ���·��
		    	                        continue;}
		    	                    }
		    	                    long arrival_u_time = query_range_end;
		    	                    for(int j=Search_Edge_Quadruple(arrival_u_time, map_link0.get(u), 0); j < map_link0.get(u).m_num; j++){
		    	                        // use binary search to get correct position
		    	                    	int busid = map_link0.get(u).get(j).busid;
		    	                    	if(busid==u_bus)
		    	                    		walk_time=0;
		    	                    	else
		    	                    		walk_time=3;
		    	                        if(map_link0.get(u).get(j).ta <arrival_u_time+walk_time) continue;
		    	                        int w = map_link0.get(u).get(j).v;
		    							if(map_leveltovertexid[w]<=i)continue;
		    	                        long taa = map_link0.get(u).get(j).ta;
		    	                        long tbb = map_link0.get(u).get(j).tb;
		    	                        
		    	                        int father = u;
		    	                        /*if(mark.exist(w) &&  mark[w] < tbb) {
		    	                          continue;
		    	                          }*/
		    	                        long w_start_time = start_time, w_end_time;
		    	                        if( mark.exist(w)){
		    	                            w_end_time = mark.m_data[w];
		    	                        }else{
		    	                            w_end_time = Long.MAX_VALUE-3;
		    	                        }		                     
		    	                        if(tbb-start_time<w_end_time-w_start_time||tbb-start_time==w_end_time-w_start_time
		    								&&(w_[w]==-1||w_[father]>=0&&w_[father]<w_[w]||map_leveltovertexid[father]<w_[w]))
		    	                        {
		    	                            mark.insert(w,  (int) tbb);///!!!����������w�Ĺ淶·��������ʱ����ͬ��·�������ж�����w��μ�¼��mark��occur����,��ʹ����ʱ�䲻һ��Ҳ��¼������ʵ��Υ�������ĵ�֧������
		    	                            nodes_pq0.insert(w, tbb - start_time,busid);
		    	                            if(busmark.m_data[father] == busid) 
		    	                            	busmark.insert(w, busid);
		    	                            else
		    	                            	busmark.insert(w, -1);
		    	                            		                         		    	                          		    	                            
		    	                            fathermark.insert(w, father);
//		    								if(tbb-start_time==w_end_time-w_start_time)printf("%d",w_[w]);
		    								w_[w]=w_[father];
		    								if(w_[w]<0||map_leveltovertexid[father]<w_[w])
		    									w_[w]=map_leveltovertexid[father];
//		    								if(tbb-start_time==w_end_time-w_start_time)printf(" %d\n",w_[w]);
		    	                        }
		    	                    }
		    	                }

		    	                for( int counter =0; counter <mark.occur.m_num; counter++){
		    	                    if(mark.exist(mark.occur.get(counter))){
		    	                    	
		    	                        int j = mark.occur.get(counter);
		    	                        if(j==3)
		    	                    		{int x;
		    	                    		x= 44;
		    	                    		}
		    	                        
		    	                        if(j!=v){
		    	                        	int ta = start_time;
		    	                        	int tb = mark.m_data[j];
		    	                            int father = (int) fathermark.m_data[j];
		    	                            int busid = (int) busmark.m_data[j];
		    	                            assert(father>=0);
		    	                            LabelQunituple new_p=new LabelQunituple(map_leveltovertexid[v],ta, tb, map_leveltovertexid[father], w_[j], busid,startBus); 
		    	                            //label[1][map_leveltovertexid[j]].sorted_insert(new_p);
		    	                            add_Label_for_map(map_label1.get(map_leveltovertexid[j]), new_p); 
//		    								printf("%d in: %d %d %d %d %d\n",j,v,ta,tb,father,busid);
		    	                            //lcnt++;
		    	                        }
		    	                    }
		    	                }
		    	                mark.clean();
		    	                busmark.clean();
		    	                fathermark.clean();
		    	                nodes_pq0.clean();
    	            
		            }
		            
		            //backward search
		            mark.clean();
		            for(k=0; k<map_link1.get(v).m_num; k++)
		            {
		            	int end_time =  map_link1.get(v).get(k).tb;
		                int endBus=map_link1.get(v).get(k).busid;
		            	mark.insert(v, (int) end_time );  
		 		    	int same_timestamp_vertex=map_link1.get(v).get(k).v;
		 		    	if(map_leveltovertexid[same_timestamp_vertex]<=i)
		 		    		continue;
		 		    	nodes_pq1.clean();
		 		    	
		 		    	
		            	for(ii=0;ii<N;ii++)
		            		w_[ii]=-1;
		            	//if(v==3)
		            	//	System.out.println("debug");
		            	
		                for( ; k< map_link1.get(v).m_num && end_time == map_link1.get(v).get(k).tb ; k++){
		                  
		                  
		                    
		                  long  ts = map_link1.get(v).get(k).ta;

		                    mark.insert(same_timestamp_vertex, (int) ts );
		                    busmark.insert(same_timestamp_vertex, map_link1.get(v).get(k).busid);
		                    fathermark.insert(same_timestamp_vertex, v);
		                    nodes_pq1.insert(same_timestamp_vertex, end_time -ts,map_link1.get(v).get(k).busid);
							w_[same_timestamp_vertex]=-2;
		                }
		                k--;
		                LabelQunituple p=new LabelQunituple(map_leveltovertexid[v], map_link1.get(v).get(k).tb, map_link1.get(v).get(k).tb, -1, -2, -1,endBus);
		                //label[1][map_leveltovertexid[v]].sorted_insert(p);
		                add_Label_for_map(map_label1.get(map_leveltovertexid[v]), p);

		                while(!nodes_pq1.empty()){
		                    int u = nodes_pq1.head();
		                    int u_bus=nodes_pq1.bus();
		                    nodes_pq1.pop();
		                    long query_range_start, query_range_end=end_time;
		                    if( mark.exist(u)){
		                        query_range_start = mark.m_data[u];
		                    }else{
		                        query_range_start = -1;
		                    }
		                    int src = map_leveltovertexid[v];
    	        	        int tar = map_leveltovertexid[u];
		                    boolean reachable = reachable_query_for_map(tar,src, query_range_start, query_range_end,map_label0,map_label1);
		                    if(reachable){
		                        if(mark.exist(u))
		                        {     mark.erase(u);//ȥ����ʱ�����һ���·��
		                        continue;}
		                    }
		                    long start_from_u_time = query_range_start;
		                    for(int j=Search_Edge_Quadruple(start_from_u_time, map_link1.get(u), 1); j >=0; j--){
		                        //use binary search to get correct position
		                    	int busid = map_link1.get(u).get(j).busid;
		                    	if(busid==u_bus)
		                    		walk_time=0;
		                    	else
		                    		walk_time=3;
		                    	if(map_link1.get(u).get(j).tb +walk_time> start_from_u_time ) continue;
		                        int w = map_link1.get(u).get(j).v;
		                        if(map_leveltovertexid[w]<=i)continue;
		                        long taa = map_link1.get(u).get(j).ta;
		                        long tbb = map_link1.get(u).get(j).tb;
		                        long w_start_time, w_end_time=end_time;
		                        
		                        int father = u;
		                        if(mark.exist(w)){
		                            w_start_time = mark.m_data[w];
		                        }else{
		                            w_start_time = -1;
		                        }
	  	
		                        if(end_time-taa<w_end_time-w_start_time||end_time-taa==w_end_time-w_start_time
									&&(w_[w]==-1||w_[father]>=0&&w_[father]<w_[w]||map_leveltovertexid[father]<w_[w])){
		                            
	                            ///
		                            
		                            if(busmark.m_data[father] == busid)
		                            {
		                            	busmark.insert(w, busid);
		                            	
		                            }
		                            else 
		                            {
		                            	busmark.insert(w, -1);
		                            	
		                            }
		                           // mark.insert(w, (int) taa-walk_time);
		                            //nodes_pq[1].insert(w, end_time - taa);
		                            ///
		                        			                    		                        	
		                        	mark.insert(w, (int) taa);
		                            nodes_pq1.insert(w, end_time - taa,busid);
		                           // if(busmark.m_data[father] == busid)busmark.insert(w, busid);
		                            //else busmark.insert(w,-1);
		                            fathermark.insert(w,father);
//									if(end_time-taa==w_end_time-w_start_time)printf("%d",w_[w]);
									w_[w]=w_[father];
									if(w_[w]<0||map_leveltovertexid[father]<w_[w])w_[w]=map_leveltovertexid[father];
//									if(end_time-taa==w_end_time-w_start_time)printf(" %d\n",w_[w]);
		                        }
		                    }
		                }
		                for( int counter = 0; counter < mark.occur.m_num; counter++){
		                    if(mark.exist(mark.occur.get(counter))){
		                        int j = mark.occur.get(counter);
		                        if(j!=v){
		                        	int ta = mark.m_data[j];
		                        	int tb = end_time;
		                            
		                            int father = (int) fathermark.m_data[j];
		                            int busid = (int) busmark.m_data[j];
		                            assert(father>=0);
		                            LabelQunituple new_p=new LabelQunituple(map_leveltovertexid[v],ta, tb, map_leveltovertexid[father], w_[j], busid,endBus);
		                            add_Label_for_map(map_label0.get(map_leveltovertexid[j]), new_p);
//									printf("%d out: %d %d %d %d %d\n",j,v,ta,tb,father,busid);
		                            //label[0][map_leveltovertexid[j]].sorted_insert(new_p);
		                            lcnt++;
		                        }
		                    }
		                }
		                mark.clean();
		                busmark.clean();
		                fathermark.clean();
		                nodes_pq1.clean();
		                
		                //context.write(null,null);
		            
		            	
		            }
	    			
	    			

	    			 	 
	    		}
	    		

	    	}
	    
	    }
	    
	    public static  class CombineMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	
	    	//private final static IntWritable mapOutputValue=new IntWritable(1);
	    	//private Text mapOutputKey=new Text();
	    	Vector2 map_link0;	
	    	Vector2 map_link1;	
	    	Vector3_for_map map_label0;
	    	Vector3_for_map map_label1;
	    	iMap mark;
	    	iMap busmark;
	   	 	iMap fathermark;
	    	int map_leveltovertexid[];
	    	int map_vertexid2level[];
	    	iHeap nodes_pq0;
	    	iHeap nodes_pq1;
	    	int N;
	    	  @Override
	          protected void setup(Context context)
	                  throws IOException, InterruptedException {
	    		 // Configuration conf = context.getConfiguration();
	    		 // N = conf.getInt("nNodes", -1);
	    		  
	    		  map_link0=new Vector2();
	    		  map_link1=new Vector2();
	    		  HHL_temporal2.load_temporal_graph_for_austin(map_link0,map_link1);
	    		  //HHL_temporal2.load_temporal_graph_for_map12(map_link0,map_link1);
	    		  //map_label=HHL_temporal2.label1;
	    		  N=map_link0.m_num;
	    		  map_label0=new Vector3_for_map();
	    		  
	    		  map_label0.re_allocate(N);
	    		  map_label0.m_num = N;
	    		  
	    		  map_label1=new Vector3_for_map();
	    		  
	    		  map_label1.re_allocate(N);
	    		  map_label1.m_num = N;
	    		  
	    		  map_leveltovertexid=new int[N];
	    		  map_vertexid2level=new int[N];
	    		  mark= new iMap();
	    		  busmark= new iMap();
	  	   	 	  fathermark= new iMap();
	    		  nodes_pq0=new iHeap();
	    		  nodes_pq0.initialize(N);
	    		  nodes_pq1=new iHeap();
	    		  nodes_pq1.initialize(N);
	    		  mark.initialize(N);
	  			  busmark.initialize(N);
	  			  fathermark.initialize(N);
//	  			  for(int i=0; i<N; i++){
//    	            map_leveltovertexid[i] = i;
//    	            map_vertexid2level[i]=i;
//    	         }
	  			  
	  			  
	    		  map_vertexid2level=HHL_temporal2.load_order_file_for_austin("/opt/TTL_source/austin.order",N);
	    		  for(int i=0; i<N; i++){
	    	            map_leveltovertexid[map_vertexid2level[i]] = i;
	    	        }
	    		  //map_leveltovertexid=HHL_temporal2.leveltovertexid;
	    		 // map_vertexid2level=HHL_temporal2.vertexid2level;
	          }
	    	  @Override
	    	  protected void cleanup(Context context) throws IOException,
	    	  InterruptedException {
	    		  for(int i=0;i<map_label0.m_num;i++)
		            	if(map_label0.get(i).m_num>0)
		            	{
		            		String longStr="";
		            		for(int j=0;j<map_label0.get(i).m_num;j++)
		            		{
		            			longStr+="-7 "+Integer.toString(map_label0.get(i).get(j).m_num)+" ";///-7 is for label_out
		            			for(int k=0;k<map_label0.get(i).get(j).m_num;k++)
		            			{
		            				String v=Integer.toString(map_label0.get(i).get(j).get(k).v);
		    						String ta=Integer.toString( map_label0.get(i).get(j).get(k).ta);
		    						String tb=Integer.toString( (map_label0.get(i).get(j).get(k).tb));
		    						String father=Integer.toString(map_label0.get(i).get(j).get(k).father);
		    						String ww=Integer.toString(map_label0.get(i).get(j).get(k).w);
		    						String busid=Integer.toString(map_label0.get(i).get(j).get(k).busid);
		    						String ebus=Integer.toString(map_label0.get(i).get(j).get(k).endOrStartBus);
		    						String s=v+" "+ta+" "+tb+" "+father+" "+ww+" "+busid+" "+ebus+" ";
		    						longStr+=s;
		            			}
		            		
				            	IntWritable iw=new IntWritable(i);
				            	//longStr+="-1 ";
				            	Text valueStr=new Text(longStr);
				            	context.write(iw,valueStr);
				            	longStr=null;
				            	longStr="";
		            		}
		            		
		            		
		            	}
	    		  
	    		  
	    		  
	    		  for(int i=0;i<map_label1.m_num;i++)
		            	if(map_label1.get(i).m_num>0)
		            	{
		            		String longStr="";
		            		for(int j=0;j<map_label1.get(i).m_num;j++)
		            		{
		            			longStr+="-8 "+Integer.toString(map_label1.get(i).get(j).m_num)+" ";///-8 is for label_in
		            			for(int k=0;k<map_label1.get(i).get(j).m_num;k++)
		            			{
		            				String v=Integer.toString(map_label1.get(i).get(j).get(k).v);
		    						String ta=Integer.toString( map_label1.get(i).get(j).get(k).ta);
		    						String tb=Integer.toString( (map_label1.get(i).get(j).get(k).tb));
		    						String father=Integer.toString(map_label1.get(i).get(j).get(k).father);
		    						String ww=Integer.toString(map_label1.get(i).get(j).get(k).w);
		    						String busid=Integer.toString(map_label1.get(i).get(j).get(k).busid);
		    						String ebus=Integer.toString(map_label1.get(i).get(j).get(k).endOrStartBus);
		    						String s=v+" "+ta+" "+tb+" "+father+" "+ww+" "+busid+" "+ebus+" ";
		    						longStr+=s;
		            			}
		            		
				            	IntWritable iw=new IntWritable(i);
				            	//longStr+="-1 ";
				            	Text valueStr=new Text(longStr);
				            	longStr=null;
				            	longStr="";
				            	context.write(iw,valueStr);			            	
		            		}
		            		
		            		
		            	}
	    	  }
	    	  
	    	  
	    	
	    	@Override
	    	protected void map(LongWritable key,  Text value, Context context)
			throws IOException, InterruptedException {
		
	    		int walk_time;
	    		String line=value.toString();
	    		//System.out.println("====="+line+"=====");
	    		
	    		StringTokenizer stringTokenizer=new StringTokenizer(line);
	    		while(stringTokenizer.hasMoreTokens()){
	    			String v1=stringTokenizer.nextToken();
	    			//String e=stringTokenizer.nextToken();
	   
	    			int i=Integer.parseInt(v1);
	    			int k;
	    
	    			//if(i>10)
	    				//continue;

	    			int w_[]=new int[N];
	    			int ii;
	    			
	    			

    	            int v = map_vertexid2level[i];
	    			mark.clean();
		            for( k=map_link0.get(v).m_num-1; k>=0; k--){	        	         
		    	            //forward search
		    	            
		    	            if(map_leveltovertexid[map_link0.get(v).get(k).v]<=i)
		    	            	continue;
		    	            nodes_pq0.clean();
		    	            
		    	            for(ii=0;ii<N;ii++)
		    	            	w_[ii]=-1;
		    					//links[v][k]�е�k�Ƕ���v�ĵ�k�����ߣ���������ָ��Ķ���id��links[v][k].v
		    	                int start_time = map_link0.get(v).get(k).ta;
		    	                int startBus=map_link0.get(v).get(k).busid;///��ѯƥ��ʱlabel_in��Ҫ֪��·���ϵ�һ̨������
		    	                mark.insert(v, (int)start_time);
		    	              
		    	                for( ; k>=0 && start_time == map_link0.get(v).get(k).ta ; k--){///����ʱ����ͬ�����Ų�ͬ������ȥ�����Գ���ʱ����ͬ�ı�
		    	                    int same_timestamp_vertex = map_link0.get(v).get(k).v;
		    						if(map_leveltovertexid[same_timestamp_vertex]<=i)continue;
		    	                    long ts = map_link0.get(v).get(k).tb;
		    	                    mark.insert(same_timestamp_vertex, (int) ts); 
		    	                    busmark.insert(same_timestamp_vertex, map_link0.get(v).get(k).busid);
		    	                    fathermark.insert(same_timestamp_vertex, v);
		    	                    nodes_pq0.insert(same_timestamp_vertex, ts-start_time,map_link0.get(v).get(k).busid);
		    						w_[same_timestamp_vertex]=-2;
		    	                }
		    	               k++;
		    	                LabelQunituple p=new LabelQunituple(map_leveltovertexid[v], start_time, start_time, -1, -2, -1,startBus);
		    	                add_Label_for_map( map_label0.get(map_leveltovertexid[v]), p );
		    	                //label[0][map_leveltovertexid[v]].sorted_insert(p);
		    	                while(!nodes_pq0.empty()){
		    	                    int u = nodes_pq0.head();
		    	                    int u_bus=nodes_pq0.bus();
		    	                    nodes_pq0.pop();
		    	                    long query_range_start = start_time, query_range_end;
		    	                    if(mark.exist(u)){
		    	                        query_range_end = mark.m_data[u];
		    	                    }else{
		    	                        query_range_end = Long.MAX_VALUE-3;
		    	                    }
		    	                    int src = map_leveltovertexid[v];///the original reachable_query() has to do this
		    	        	        int tar = map_leveltovertexid[u];
		    	                   boolean reachable = reachable_query_for_map(src,tar, query_range_start, query_range_end,map_label0,map_label1);
		    	                   if(reachable){
		    	                      if(mark.exist(u))
		    	                        {   mark.erase(u);//ȥ����ʱ�����һ���·��
		    	                        continue;}
		    	                    }
		    	                    long arrival_u_time = query_range_end;
		    	                    for(int j=Search_Edge_Quadruple(arrival_u_time, map_link0.get(u), 0); j < map_link0.get(u).m_num; j++){
		    	                        // use binary search to get correct position
		    	                    	int busid = map_link0.get(u).get(j).busid;
		    	                    	if(busid==u_bus)
		    	                    		walk_time=0;
		    	                    	else
		    	                    		walk_time=3;
		    	                        if(map_link0.get(u).get(j).ta <arrival_u_time+walk_time) continue;
		    	                        int w = map_link0.get(u).get(j).v;
		    							if(map_leveltovertexid[w]<=i)continue;
		    	                        long taa = map_link0.get(u).get(j).ta;
		    	                        long tbb = map_link0.get(u).get(j).tb;
		    	                        
		    	                        int father = u;
		    	                        /*if(mark.exist(w) &&  mark[w] < tbb) {
		    	                          continue;
		    	                          }*/
		    	                        long w_start_time = start_time, w_end_time;
		    	                        if( mark.exist(w)){
		    	                            w_end_time = mark.m_data[w];
		    	                        }else{
		    	                            w_end_time = Long.MAX_VALUE-3;
		    	                        }		                     
		    	                        if(tbb-start_time<w_end_time-w_start_time||tbb-start_time==w_end_time-w_start_time
		    								&&(w_[w]==-1||w_[father]>=0&&w_[father]<w_[w]||map_leveltovertexid[father]<w_[w]))
		    	                        {
		    	                            mark.insert(w,  (int) tbb);///!!!����������w�Ĺ淶·��������ʱ����ͬ��·�������ж�����w��μ�¼��mark��occur����,��ʹ����ʱ�䲻һ��Ҳ��¼������ʵ��Υ�������ĵ�֧������
		    	                            nodes_pq0.insert(w, tbb - start_time,busid);
		    	                            if(busmark.m_data[father] == busid) 
		    	                            	busmark.insert(w, busid);
		    	                            else
		    	                            	busmark.insert(w, -1);
		    	                            		                         		    	                          		    	                            
		    	                            fathermark.insert(w, father);
//		    								if(tbb-start_time==w_end_time-w_start_time)printf("%d",w_[w]);
		    								w_[w]=w_[father];
		    								if(w_[w]<0||map_leveltovertexid[father]<w_[w])
		    									w_[w]=map_leveltovertexid[father];
//		    								if(tbb-start_time==w_end_time-w_start_time)printf(" %d\n",w_[w]);
		    	                        }
		    	                    }
		    	                }

		    	                for( int counter =0; counter <mark.occur.m_num; counter++){
		    	                    if(mark.exist(mark.occur.get(counter))){
		    	                    	
		    	                        int j = mark.occur.get(counter);
		    	                        if(j==3)
		    	                    		{int x;
		    	                    		x= 44;
		    	                    		}
		    	                        
		    	                        if(j!=v){
		    	                        	int ta = start_time;
		    	                        	int tb = mark.m_data[j];
		    	                            int father = (int) fathermark.m_data[j];
		    	                            int busid = (int) busmark.m_data[j];
		    	                            assert(father>=0);
		    	                            LabelQunituple new_p=new LabelQunituple(map_leveltovertexid[v],ta, tb, map_leveltovertexid[father], w_[j], busid,startBus); 
		    	                            //label[1][map_leveltovertexid[j]].sorted_insert(new_p);
		    	                            add_Label_for_map(map_label1.get(map_leveltovertexid[j]), new_p); 
//		    								printf("%d in: %d %d %d %d %d\n",j,v,ta,tb,father,busid);
		    	                            //lcnt++;
		    	                        }
		    	                    }
		    	                }
		    	                mark.clean();
		    	                busmark.clean();
		    	                fathermark.clean();
		    	                nodes_pq0.clean();
    	            
		            }
		            
		            //backward search
		            mark.clean();
		            for(k=0; k<map_link1.get(v).m_num; k++)
		            {
		            	int end_time =  map_link1.get(v).get(k).tb;
		                int endBus=map_link1.get(v).get(k).busid;
		            	mark.insert(v, (int) end_time );  
		 		    	
		 		    	
		 		    	nodes_pq1.clean();
		 		    	
		 		    	
		            	for(ii=0;ii<N;ii++)
		            		w_[ii]=-1;
		            	//if(v==3)
		            	//	System.out.println("debug");
		            	
		                for( ; k< map_link1.get(v).m_num && end_time == map_link1.get(v).get(k).tb ; k++){
		                  
		                	int same_timestamp_vertex=map_link1.get(v).get(k).v;
		                	if(map_leveltovertexid[same_timestamp_vertex]<=i)
			 		    		continue;
		                  long  ts = map_link1.get(v).get(k).ta;

		                    mark.insert(same_timestamp_vertex, (int) ts );
		                    busmark.insert(same_timestamp_vertex, map_link1.get(v).get(k).busid);
		                    fathermark.insert(same_timestamp_vertex, v);
		                    nodes_pq1.insert(same_timestamp_vertex, end_time -ts,map_link1.get(v).get(k).busid);
							w_[same_timestamp_vertex]=-2;
		                }
		                k--;
		                LabelQunituple p=new LabelQunituple(map_leveltovertexid[v], map_link1.get(v).get(k).tb, map_link1.get(v).get(k).tb, -1, -2, -1,endBus);
		                //label[1][map_leveltovertexid[v]].sorted_insert(p);
		                add_Label_for_map(map_label1.get(map_leveltovertexid[v]), p);

		                while(!nodes_pq1.empty()){
		                    int u = nodes_pq1.head();
		                    int u_bus=nodes_pq1.bus();
		                    nodes_pq1.pop();
		                    long query_range_start, query_range_end=end_time;
		                    if( mark.exist(u)){
		                        query_range_start = mark.m_data[u];
		                    }else{
		                        query_range_start = -1;
		                    }
		                    int src = map_leveltovertexid[v];
    	        	        int tar = map_leveltovertexid[u];
		                    boolean reachable = reachable_query_for_map(tar,src, query_range_start, query_range_end,map_label0,map_label1);
		                    if(reachable){
		                        if(mark.exist(u))
		                        {     mark.erase(u);//ȥ����ʱ�����һ���·��
		                        continue;}
		                    }
		                    long start_from_u_time = query_range_start;
		                    for(int j=Search_Edge_Quadruple(start_from_u_time, map_link1.get(u), 1); j >=0; j--){
		                        //use binary search to get correct position
		                    	int busid = map_link1.get(u).get(j).busid;
		                    	if(busid==u_bus)
		                    		walk_time=0;
		                    	else
		                    		walk_time=3;
		                    	if(map_link1.get(u).get(j).tb +walk_time> start_from_u_time ) continue;
		                        int w = map_link1.get(u).get(j).v;
		                        if(map_leveltovertexid[w]<=i)continue;
		                        long taa = map_link1.get(u).get(j).ta;
		                        long tbb = map_link1.get(u).get(j).tb;
		                        long w_start_time, w_end_time=end_time;
		                        
		                        int father = u;
		                        if(mark.exist(w)){
		                            w_start_time = mark.m_data[w];
		                        }else{
		                            w_start_time = -1;
		                        }
	  	
		                        if(end_time-taa<w_end_time-w_start_time||end_time-taa==w_end_time-w_start_time
									&&(w_[w]==-1||w_[father]>=0&&w_[father]<w_[w]||map_leveltovertexid[father]<w_[w])){
		                            
	                            ///
		                            
		                            if(busmark.m_data[father] == busid)
		                            {
		                            	busmark.insert(w, busid);
		                            	
		                            }
		                            else 
		                            {
		                            	busmark.insert(w, -1);
		                            	
		                            }
		                           // mark.insert(w, (int) taa-walk_time);
		                            //nodes_pq[1].insert(w, end_time - taa);
		                            ///
		                        			                    		                        	
		                        	mark.insert(w, (int) taa);
		                            nodes_pq1.insert(w, end_time - taa,busid);
		                           // if(busmark.m_data[father] == busid)busmark.insert(w, busid);
		                            //else busmark.insert(w,-1);
		                            fathermark.insert(w,father);
//									if(end_time-taa==w_end_time-w_start_time)printf("%d",w_[w]);
									w_[w]=w_[father];
									if(w_[w]<0||map_leveltovertexid[father]<w_[w])w_[w]=map_leveltovertexid[father];
//									if(end_time-taa==w_end_time-w_start_time)printf(" %d\n",w_[w]);
		                        }
		                    }
		                }
		                for( int counter = 0; counter < mark.occur.m_num; counter++){
		                    if(mark.exist(mark.occur.get(counter))){
		                        int j = mark.occur.get(counter);
		                        if(j!=v){
		                        	int ta = mark.m_data[j];
		                        	int tb = end_time;
		                            
		                            int father = (int) fathermark.m_data[j];
		                            int busid = (int) busmark.m_data[j];
		                            assert(father>=0);
		                            LabelQunituple new_p=new LabelQunituple(map_leveltovertexid[v],ta, tb, map_leveltovertexid[father], w_[j], busid,endBus);
		                            add_Label_for_map(map_label0.get(map_leveltovertexid[j]), new_p);
//									printf("%d out: %d %d %d %d %d\n",j,v,ta,tb,father,busid);
		                            //label[0][map_leveltovertexid[j]].sorted_insert(new_p);
		                            lcnt++;
		                        }
		                    }
		                }
		                mark.clean();
		                busmark.clean();
		                fathermark.clean();
		                nodes_pq1.clean();
		                
		                //context.write(null,null);
		            
		            	
		            }
	    			
	    			

	    			 	 
	    		}
	    		

	    	}
	    }
	    	
	    public static class MyReducer extends
		Reducer<IntWritable, Text, IntWritable, Text> {
	
	    	
	
	    	@Override
	    	protected void reduce(IntWritable key, Iterable<Text> values,
	    			Context context) throws IOException, InterruptedException {
		
	    		Text longText=new Text();
	    		String longStr="";
	    		for(Text value:values){
	    			longStr+=value.toString();
	    			//System.out.println(longStr);
	    		}
	    		longStr+="-1";//end of line
	    		longText.set(longStr);
	    		context.write(key, longText);

	    	}

	    }
	    
	    public static class MyReducer2 extends
		Reducer<IntWritable, Text, IntWritable, Text> {
	
	    	
	
	    	@Override
	    	protected void reduce(IntWritable key, Iterable<Text> values,
	    			Context context) throws IOException, InterruptedException {
		
	    		Text longText=new Text();
	    		String longStr="";
	    		for(Text value:values){
	    			longStr+=value.toString();
	    		}
	    		longText.set(longStr);
	    		context.write(key, longText);

	    	}

	    }
	    
	    	 public static  class MyMapper2 extends Mapper<LongWritable, Text, IntWritable, Text> {
	    	 
	    		 
	    		Vector2 map_link;		    
	 	    	Vector3_for_map map_label;
	 	    	iMap mark;
	 	    	iMap busmark;
	 	   	 	iMap fathermark;
	 	    	int map_leveltovertexid[];
	 	    	int map_vertexid2level[];
	 	    	iHeap nodes_pq;
	 	    	int N;
	    		 
	    		 @Override
		          protected void setup(Context context)
		                  throws IOException, InterruptedException {
	    			 Configuration conf = context.getConfiguration();
		    		  N = conf.getInt("nNodes", -1);
		    		  
		    		  map_link=HHL_temporal2.load_temporal_graph_for_map2(null);
		    		 // map_label=HHL_temporal2.label0;
		    		  map_label=new Vector3_for_map();
		    		  
		    		  map_label.re_allocate(N);
		    		  map_label.m_num = N;
		    		  map_leveltovertexid=new int[64];
		    		  map_vertexid2level=new int[64];
		    		  mark= new iMap();
		    		  busmark= new iMap();
		  	   	 	  fathermark= new iMap();
		    		  nodes_pq=new iHeap();
		    		  nodes_pq.initialize(N);
		    		  mark.initialize(N);
		  			  busmark.initialize(N);
		  			  fathermark.initialize(N);
		    		  for(int i=0;i<N;i++)
		    		  {
		    			  map_leveltovertexid[i]=i;
		    			  map_vertexid2level[i]=i;
		    		  }
		    		 // HHL_temporal2.load_order_file_for_map("/opt/TTL_source/gz.order");
		          }
	    		 
	    		
	    		 
	    		 
	 	    	protected void map(LongWritable key, Text value, Context context)
	 			throws IOException, InterruptedException {
	    			
	    			int walk_time;
	    			String line=value.toString();
	 		    	
	 	    		StringTokenizer stringTokenizer=new StringTokenizer(line);
	 	    		while(stringTokenizer.hasMoreTokens()){
	 	    			String v1=stringTokenizer.nextToken();
	 	    			
	 	    			//String e2=stringTokenizer.nextToken();
	 	    			int i=Integer.parseInt(v1);
	 	    			
	 	    			int k;

	 	    			
	 	    			int w_[]=new int[N];
	 	    			int ii;
		    	     
	 	    			mark.clean();
			            for( k=map_link.get(i).m_num-1; k>=0; k--){
			            	
			            	//System.out.println("==============="+map_link.m_num);
			 		    	int v = map_leveltovertexid[i];		         
			 		    	            //forward search
			 		    	int same_timestamp_vertex=map_link.get(v).get(k).v;
			 		    	if(map_leveltovertexid[same_timestamp_vertex]<=i)
			 		    		continue;
			 		    	nodes_pq.clean();
			 		    	mark.clean();
			 		    	
			            	for(ii=0;ii<N;ii++)
			            		w_[ii]=-1;
			            	//if(v==3)
			            	//	System.out.println("debug");
			            	int end_time =  map_link.get(v).get(k).tb;
			                int endBus=map_link.get(v).get(k).busid;///��ѯƥ��ʱlabel_out��Ҫ֪��·���ϵ��������
			                mark.insert(v, (int) end_time );
			               /// for( ; k< map_link.get(v).m_num && end_time == map_link.get(v).get(k).tb ; k++){
			                  
			                  
			                    
			                  long  ts = map_link.get(v).get(k).ta;

			                    mark.insert(same_timestamp_vertex, (int) ts );
			                    busmark.insert(same_timestamp_vertex, map_link.get(v).get(k).busid);
			                    fathermark.insert(same_timestamp_vertex, v);
			                    nodes_pq.insert(same_timestamp_vertex, end_time -ts,map_link.get(v).get(k).busid);
								w_[same_timestamp_vertex]=-2;
			              ///  }
			               /// k--;
			                LabelQunituple p=new LabelQunituple(map_leveltovertexid[v], map_link.get(v).get(k).tb, map_link.get(v).get(k).tb, -1, -2, -1,endBus);
			                //label[1][map_leveltovertexid[v]].sorted_insert(p);
			                //add_Label(map_label.get(map_leveltovertexid[v]), p);

			                while(!nodes_pq.empty()){
			                    int u = nodes_pq.head();
			                    int u_bus=nodes_pq.bus();
			                    nodes_pq.pop();
			                    long query_range_start, query_range_end=end_time;
			                    if( mark.exist(u)){
			                        query_range_start = mark.m_data[u];
			                    }else{
			                        query_range_start = -1;
			                    }
			                   /// boolean reachable = reachable_query(u,v, query_range_start, query_range_end);
			                   /// if(reachable){
			                      //  if(mark.exist(u))
			                     //   {     mark.erase(u);//ȥ����ʱ�����һ���·��
			                      //  continue;}
			                   /// }
			                    long start_from_u_time = query_range_start;
			                    for(int j=Search_Edge_Quadruple(start_from_u_time, map_link.get(u), 1); j >=0; j--){
			                        //use binary search to get correct position
			                    	int busid = map_link.get(u).get(j).busid;
			                    	if(busid==u_bus)
			                    		walk_time=0;
			                    	else
			                    		walk_time=3;
			                    	if(map_link.get(u).get(j).tb +walk_time> start_from_u_time ) continue;
			                        int w = map_link.get(u).get(j).v;
			                        if(map_leveltovertexid[w]<=i)continue;
			                        long taa = map_link.get(u).get(j).ta;
			                        long tbb = map_link.get(u).get(j).tb;
			                        long w_start_time, w_end_time=end_time;
			                        
			                        int father = u;
			                        if(mark.exist(w)){
			                            w_start_time = mark.m_data[w];
			                        }else{
			                            w_start_time = -1;
			                        }
		  	
			                        if(end_time-taa<w_end_time-w_start_time||end_time-taa==w_end_time-w_start_time
										&&(w_[w]==-1||w_[father]>=0&&w_[father]<w_[w]||map_leveltovertexid[father]<w_[w])){
			                            
		                            ///
			                            
			                            if(busmark.m_data[father] == busid)
			                            {
			                            	busmark.insert(w, busid);
			                            	
			                            }
			                            else 
			                            {
			                            	busmark.insert(w, -1);
			                            	
			                            }
			                           // mark.insert(w, (int) taa-walk_time);
			                            //nodes_pq[1].insert(w, end_time - taa);
			                            ///
			                        			                    		                        	
			                        	mark.insert(w, (int) taa);
			                            nodes_pq.insert(w, end_time - taa,busid);
			                           // if(busmark.m_data[father] == busid)busmark.insert(w, busid);
			                            //else busmark.insert(w,-1);
			                            fathermark.insert(w,father);
//										if(end_time-taa==w_end_time-w_start_time)printf("%d",w_[w]);
										w_[w]=w_[father];
										if(w_[w]<0||map_leveltovertexid[father]<w_[w])w_[w]=map_leveltovertexid[father];
//										if(end_time-taa==w_end_time-w_start_time)printf(" %d\n",w_[w]);
			                        }
			                    }
			                }
			                for( int counter = 0; counter < mark.occur.m_num; counter++){
			                    if(mark.exist(mark.occur.get(counter))){
			                        int j = mark.occur.get(counter);
			                        if(j!=v){
			                        	int ta = mark.m_data[j];
			                        	int tb = end_time;
			                            
			                            int father = (int) fathermark.m_data[j];
			                            int busid = (int) busmark.m_data[j];
			                            assert(father>=0);
			                            LabelQunituple new_p=new LabelQunituple(map_leveltovertexid[v],ta, tb, map_leveltovertexid[father], w_[j], busid,endBus);
			                            add_Label_for_map(map_label.get(map_leveltovertexid[j]), new_p);
//										printf("%d out: %d %d %d %d %d\n",j,v,ta,tb,father,busid);
			                            //label[0][map_leveltovertexid[j]].sorted_insert(new_p);
			                            lcnt++;
			                        }
			                    }
			                }
			                mark.clean();
			                busmark.clean();
			                fathermark.clean();
			                nodes_pq.clean();
			                
			                //context.write(null,null);
			            }
	 	    			
			            
//			            for(i=0;i<map_label.m_num;i++)
//			            	if(map_label.get(i).m_num>0)
//			            	{
//			            		String longStr="";
//			            		for(int j=0;j<map_label.get(i).m_num;j++)
//			            		{
//			            			longStr=Integer.toString(map_label.get(i).get(j).m_num)+" ";
//			            			for(k=0;k<map_label.get(i).get(j).m_num;k++)
//			            			{
//			            				String v=Integer.toString(map_label.get(i).get(j).get(k).v);
//			    						String ta=Integer.toString((int) map_label.get(i).get(j).get(k).ta);
//			    						String tb=Integer.toString((int) (map_label.get(i).get(j).get(k).tb));
//			    						String father=Integer.toString(map_label.get(i).get(j).get(k).father);
//			    						String busid=Integer.toString(map_label.get(i).get(j).get(k).busid);
//			    						String s=v+" "+ta+" "+tb+" "+father+" "+busid+" ";
//			    						longStr+=s;
//			            			}
//			            		
//					            	IntWritable iw=new IntWritable(i);
//					            	longStr+="-1";
//					            	Text valueStr=new Text(longStr);
//					            	context.write(iw,valueStr);
//			            		}
//			            		
//			            		
//			            	}
		    	     
	 	    			
			        
	 	    		}
	 	    		
		            for(int i=0;i<map_label.m_num;i++)
		            	if(map_label.get(i).m_num>0)
		            	{
		            		String longStr="";
		            		for(int j=0;j<map_label.get(i).m_num;j++)
		            		{
		            			longStr=Integer.toString(map_label.get(i).get(j).m_num)+" ";
		            			for(int k=0;k<map_label.get(i).get(j).m_num;k++)
		            			{
		            				String v=Integer.toString(map_label.get(i).get(j).get(k).v);
		    						String ta=Integer.toString((int) map_label.get(i).get(j).get(k).ta);
		    						String tb=Integer.toString((int) (map_label.get(i).get(j).get(k).tb));
		    						String father=Integer.toString(map_label.get(i).get(j).get(k).father);
		    						String busid=Integer.toString(map_label.get(i).get(j).get(k).busid);
		    						String s=v+" "+ta+" "+tb+" "+father+" "+busid+" ";
		    						longStr+=s;
		            			}
		            		
				            	IntWritable iw=new IntWritable(i);
				            	longStr+="-1";
				            	Text valueStr=new Text(longStr);
				            	context.write(iw,valueStr);
		            		}
		            		
		            		
		            	}
	 	    		
	    	
	 	    		
	 	    		
	 	    		
	    }

	  }
	    
	    
	    
	 void compute_index_with_order_BFS(){
			int w_[]=new int[N];
			int ii;
			int walk_time;
			lcnt=0;
	        func();
	        iHeap nodes_pq[]=new iHeap[2];
	        nodes_pq[0]=new iHeap();
	        nodes_pq[1]=new iHeap();
	        nodes_pq[0].initialize(N);
	        nodes_pq[1].initialize(N);
	        for(int i=0; i<N; i++){
	            int v = vertexid2level[i];		         
	            //forward search
	            mark.clean();
	            for(int k=links0.get(v).m_num-1; k>=0; k--){
	            	for(ii=0;ii<N;ii++)
	            		w_[ii]=-1;
					if(leveltovertexid[links0.get(v).get(k).v]<=i)continue;//links[v][k]�е�k�Ƕ���v�ĵ�k�����ߣ���������ָ��Ķ���id��links[v][k].v
					int start_time = links0.get(v).get(k).ta;
	                int startBus=links0.get(v).get(k).busid;///��ѯƥ��ʱlabel_in��Ҫ֪��·���ϵ�һ̨������
	                mark.insert(v, (int)start_time);
	              
	               /// for( ; k>=0 && start_time == links0.get(v).get(k).ta ; k--){///����ʱ����ͬ�����Ų�ͬ������ȥ�����Գ���ʱ����ͬ�ı�
	                    int same_timestamp_vertex = links0.get(v).get(k).v;
						if(leveltovertexid[same_timestamp_vertex]<=i)continue;
						int ts = links0.get(v).get(k).tb;
	                    mark.insert(same_timestamp_vertex, (int) ts); 
	                    busmark.insert(same_timestamp_vertex, links0.get(v).get(k).busid);
	                    fathermark.insert(same_timestamp_vertex, v);
	                    nodes_pq[0].insert(same_timestamp_vertex, ts-start_time,links0.get(v).get(k).busid);
						w_[same_timestamp_vertex]=-2;
	              ///  }
	             ///  k++;
	                LabelQunituple p=new LabelQunituple(leveltovertexid[v], start_time, start_time, -1, -2, -1,startBus);
	                add_Label( label0.get(leveltovertexid[v]), p );
	                //label[0][leveltovertexid[v]].sorted_insert(p);
	                while(!nodes_pq[0].empty()){
	                    int u = nodes_pq[0].head();
	                    int u_bus=nodes_pq[0].bus();
	                    nodes_pq[0].pop();
	                    long query_range_start = start_time, query_range_end;
	                    if(mark.exist(u)){
	                        query_range_end = mark.m_data[u];
	                    }else{
	                        query_range_end = Long.MAX_VALUE-3;
	                    }
	                    boolean reachable = reachable_query(v, u, query_range_start, query_range_end);
	                    if(reachable){
	                        if(mark.exist(u))
	                            mark.erase(u);//ȥ����ʱ�����һ���·��
	                        continue;
	                    }
	                    long arrival_u_time = query_range_end;
	                    for(int j=Search_Edge_Quadruple(arrival_u_time, links0.get(u), 0); j < links0.get(u).m_num; j++){
	                        // use binary search to get correct position
	                    	int busid = links0.get(u).get(j).busid;
	                    	if(busid==u_bus)
	                    		walk_time=0;
	                    	else
	                    		walk_time=3;
	                        if(links0.get(u).get(j).ta <arrival_u_time+walk_time) continue;
	                        int w = links0.get(u).get(j).v;
							if(leveltovertexid[w]<=i)continue;
	                        long taa = links0.get(u).get(j).ta;
	                        long tbb = links0.get(u).get(j).tb;
	                        
	                        int father = u;
	                        /*if(mark.exist(w) &&  mark[w] < tbb) {
	                          continue;
	                          }*/
	                        long w_start_time = start_time, w_end_time;
	                        if( mark.exist(w)){
	                            w_end_time = mark.m_data[w];
	                        }else{
	                            w_end_time = Long.MAX_VALUE-3;
	                        }		                     
	                        if(tbb-start_time<w_end_time-w_start_time||tbb-start_time==w_end_time-w_start_time
								&&(w_[w]==-1||w_[father]>=0&&w_[father]<w_[w]||leveltovertexid[father]<w_[w]))
	                        {
	                            mark.insert(w,  (int) tbb);///!!!����������w�Ĺ淶·��������ʱ����ͬ��·�������ж�����w��μ�¼��mark��occur����,��ʹ����ʱ�䲻һ��Ҳ��¼������ʵ��Υ�������ĵ�֧������
	                            nodes_pq[0].insert(w, tbb - start_time,busid);
	                            if(busmark.m_data[father] == busid) 
	                            	busmark.insert(w, busid);
	                            else
	                            	busmark.insert(w, -1);
	                            		                          
	                            

	                            
	                            fathermark.insert(w, father);
//								if(tbb-start_time==w_end_time-w_start_time)printf("%d",w_[w]);
								w_[w]=w_[father];
								if(w_[w]<0||leveltovertexid[father]<w_[w])
									w_[w]=leveltovertexid[father];
//								if(tbb-start_time==w_end_time-w_start_time)printf(" %d\n",w_[w]);
	                        }
	                    }
	                }

	                for( int counter =0; counter <mark.occur.m_num; counter++){
	                    if(mark.exist(mark.occur.get(counter))){
	                    	
	                        int j = mark.occur.get(counter);
	                        if(j==3)
	                    		{int x;
	                    		x= 44;
	                    		}
	                        
	                        if(j!=v){
	                        	int ta = start_time;
	                        	int tb = mark.m_data[j];
	                            int father = (int) fathermark.m_data[j];
	                            int busid = (int) busmark.m_data[j];
	                            assert(father>=0);
	                            LabelQunituple new_p=new LabelQunituple(leveltovertexid[v],ta, tb, leveltovertexid[father], w_[j], busid,startBus); 
	                            //label[1][leveltovertexid[j]].sorted_insert(new_p);
	                            add_Label(label1.get(leveltovertexid[j]), new_p); 
//								printf("%d in: %d %d %d %d %d\n",j,v,ta,tb,father,busid);
	                            lcnt++;
	                        }
	                    }
	                }
	                mark.clean();
	                busmark.clean();
	                fathermark.clean();
	                nodes_pq[0].clean();
	            }
	            //backward search 
	            for(int k=0; k<links1.get(v).m_num; k++){
	            	for(ii=0;ii<N;ii++)
	            		w_[ii]=-1;
	            	//if(v==3)
	            	//	System.out.println("debug");
	            	int end_time =  links1.get(v).get(k).tb;
	                int endBus=links1.get(v).get(k).busid;///��ѯƥ��ʱlabel_out��Ҫ֪��·���ϵ��������
	                mark.insert(v, (int) end_time );
	               /// for( ; k< links1.get(v).m_num && end_time == links1.get(v).get(k).tb ; k++){
	                    int same_timestamp_vertex = links1.get(v).get(k).v;
	                    if(leveltovertexid[same_timestamp_vertex]<=i)continue;
	                    long ts = links1.get(v).get(k).ta;

	                    mark.insert(same_timestamp_vertex, (int) ts );
	                    busmark.insert(same_timestamp_vertex, links1.get(v).get(k).busid);
	                    fathermark.insert(same_timestamp_vertex, v);
	                    nodes_pq[1].insert(same_timestamp_vertex, end_time -ts,links1.get(v).get(k).busid);
						w_[same_timestamp_vertex]=-2;
	              ///  }
	               /// k--;
	                LabelQunituple p=new LabelQunituple(leveltovertexid[v], links1.get(v).get(k).tb, links1.get(v).get(k).tb, -1, -2, -1,endBus);
	                //label[1][leveltovertexid[v]].sorted_insert(p);
	                add_Label(label1.get(leveltovertexid[v]), p);

	                while(!nodes_pq[1].empty()){
	                    int u = nodes_pq[1].head();
	                    int u_bus=nodes_pq[1].bus();
	                    nodes_pq[1].pop();
	                    long query_range_start, query_range_end=end_time;
	                    if( mark.exist(u)){
	                        query_range_start = mark.m_data[u];
	                    }else{
	                        query_range_start = -1;
	                    }
	                    boolean reachable = reachable_query(u,v, query_range_start, query_range_end);
	                    if(reachable){
	                        if(mark.exist(u))
	                            mark.erase(u);//ȥ����ʱ�����һ���·��
	                        continue;
	                    }
	                    long start_from_u_time = query_range_start;
	                    for(int j=Search_Edge_Quadruple(start_from_u_time, links1.get(u), 1); j >=0; j--){
	                        //use binary search to get correct position
	                    	int busid = links1.get(u).get(j).busid;
	                    	if(busid==u_bus)
	                    		walk_time=0;
	                    	else
	                    		walk_time=3;
	                    	if(links1.get(u).get(j).tb +walk_time> start_from_u_time ) continue;
	                        int w = links1.get(u).get(j).v;
	                        if(leveltovertexid[w]<=i)continue;
	                        long taa = links1.get(u).get(j).ta;
	                        long tbb = links1.get(u).get(j).tb;
	                        long w_start_time, w_end_time=end_time;
	                        
	                        int father = u;
	                        if(mark.exist(w)){
	                            w_start_time = mark.m_data[w];
	                        }else{
	                            w_start_time = -1;
	                        }
     	
	                        if(end_time-taa<w_end_time-w_start_time||end_time-taa==w_end_time-w_start_time
								&&(w_[w]==-1||w_[father]>=0&&w_[father]<w_[w]||leveltovertexid[father]<w_[w])){
	                            
                               ///
	                            
	                            if(busmark.m_data[father] == busid)
	                            {
	                            	busmark.insert(w, busid);
	                            	
	                            }
	                            else 
	                            {
	                            	busmark.insert(w, -1);
	                            	
	                            }
	                           // mark.insert(w, (int) taa-walk_time);
	                            //nodes_pq[1].insert(w, end_time - taa);
	                            ///
	                        			                    		                        	
	                        	mark.insert(w, (int) taa);
	                            nodes_pq[1].insert(w, end_time - taa,busid);
	                           // if(busmark.m_data[father] == busid)busmark.insert(w, busid);
	                            //else busmark.insert(w,-1);
	                            fathermark.insert(w,father);
//								if(end_time-taa==w_end_time-w_start_time)printf("%d",w_[w]);
								w_[w]=w_[father];
								if(w_[w]<0||leveltovertexid[father]<w_[w])w_[w]=leveltovertexid[father];
//								if(end_time-taa==w_end_time-w_start_time)printf(" %d\n",w_[w]);
	                        }
	                    }
	                }
	                for( int counter = 0; counter < mark.occur.m_num; counter++){
	                    if(mark.exist(mark.occur.get(counter))){
	                        int j = mark.occur.get(counter);
	                        if(j!=v){
	                        	int ta = mark.m_data[j];
	                        	int tb = end_time;
	                            
	                            int father = (int) fathermark.m_data[j];
	                            int busid = (int) busmark.m_data[j];
	                            assert(father>=0);
	                            LabelQunituple new_p=new LabelQunituple(leveltovertexid[v],ta, tb, leveltovertexid[father], w_[j], busid,endBus);
	                            add_Label(label0.get(leveltovertexid[j]), new_p);
//								printf("%d out: %d %d %d %d %d\n",j,v,ta,tb,father,busid);
	                            //label[0][leveltovertexid[j]].sorted_insert(new_p);
	                            lcnt++;
	                        }
	                    }
	                }
	                mark.clean();
	                busmark.clean();
	                fathermark.clean();
	                nodes_pq[1].clean();
	            }
	        }
	        //children_pointer_linking();
	       // clock_t end_index_construction = clock();
	        //indexing_time = (double)(end_index_construction-start_index_construction)/CLOCKS_PER_SEC ;
	        long startTime=System.currentTimeMillis();
	        children_pointer_linking();
	        long endTime=System.currentTimeMillis();
	        double diff=(double)(endTime-startTime)/1000;
	        System.out.printf("Index children_pointer_linking Time:%.2fs\n",diff);
	        //cout<<"Total preprocessing time:"<< (ordering_time+indexing_time)<<endl;
	        //cout<<(lcnt)<<endl;
	       // cout<< (lcnt*1.0/N) <<endl;
	        return;
	    }
	

	
	static LabelQunituple get_address(int v1,int v2,long ta,long tb,Vector3 lout,Vector3 lin)
	{
		int d,v,u,l,r,m,i;
		Vector3 label;
		if(v1<v2)
		{
			label=lin;
			u=v1;
			v=v2;
		}
		else
		{
			label=lout;
			v=v1;
			u=v2;
		}
		l=-1;
		r=label.get(v).m_num;
		while(l+1<r)
		{
			m=(l+r)/2;
			if(label.get(v).get(m).get(0).v<u)l=m;
			else r=m;
		}
		if(label.get(v).get(r).get(0).v!=u)
		{
			return null;
		}
		l=-1;
		u=r;
		r=label.get(v).get(u).m_num;
		while(l+1<r)
		{
			m=(l+r)/2;
			///if(ta>=0&&label.get(v).get(u).get(m).ta<ta||tb>=0&&label.get(v).get(u).get(m).tb<tb)l=m;
			if(ta>=0&&label.get(v).get(u).get(m).ta<ta||tb>=0&&label.get(v).get(u).get(m).ta<tb)
				l=m;
			else r=m;
		}
		///
		int k;
		if(r==label.get(v).get(u).m_num)
			k=r-1;
		else
			k=r;
		if(ta==-1&&tb>=0)//��ǩ���ǰ�ta�����,��ƥ���tb���ܶ��ֵ���,��ta���ڲ���tb�ı�ǩ��ǰ�ҵ��ڲ���tb�ı�ǩ
		{
			while(k>=0&&tb!=label.get(v).get(u).get(k).tb)
			{
				k--;
			}
		}
		r=k;
		if(r<0)
			return null;
		///
		if(r>=label.get(v).get(u).m_num||ta>=0&&label.get(v).get(u).get(r).ta!=ta||tb>=0&&label.get(v).get(u).get(r).tb!=tb)
		{
			return null;
		}
		return label.get(v).get(u).get(r);
	}
	
	static void children_pointer_linking(){
        for(int i=0; i<N; i++){
            for(int j=0; j<label0.get(i).m_num; j++){
                if(label0.get(i).get(j).get(0).v==i)continue;
                for( int k=0; k<label0.get(i).get(j).m_num; k++){
                    int w=label0.get(i).get(j).get(k).w;
                    if(w>=0)
                    {
//                    	if(i==9&&j==7)
//                    	{
//                    		System.out.println("debug");
//                    	}
                        label0.get(i).get(j).get(k).iw=get_address(i,w,label0.get(i).get(j).get(k).ta,-1,label0,label1);//����w��Lable_in����ʾ��leve i��level w,��Ϊi��j��������
                        label0.get(i).get(j).get(k).wv=get_address(w,label0.get(i).get(j).get(k).v,-1,label0.get(i).get(j).get(k).tb,label0,label1);
                    }
                }
            }
            for(int j=0; j<label1.get(i).m_num; j++){
                if(label1.get(i).get(j).get(0).v==i)continue;
                for( int k=0; k<label1.get(i).get(j).m_num; k++){
                    int w=label1.get(i).get(j).get(k).w;
                    if(w>=0)
                    {
//                    	if(i==9&&j==7)
//                    	{
//                    		System.out.println("debug");
//                    	}
                    	label1.get(i).get(j).get(k).iw=get_address(label1.get(i).get(j).get(k).v,w,label1.get(i).get(j).get(k).ta,-1,label0,label1);
                        label1.get(i).get(j).get(k).wv=get_address(w,i,-1,label1.get(i).get(j).get(k).tb,label0,label1);                  
                        ///label1.get(i).get(j).get(k).iw=get_address(w,i,-1,label1.get(i).get(j).get(k).tb,label0,label1);
                        ///label1.get(i).get(j).get(k).wv=get_address(label1.get(i).get(j).get(k).v,w,label1.get(i).get(j).get(k).ta,-1,label0,label1);
                    
                    }
                }
            }
            int x=0;
            x=9;
        }
    }
	
	public class TimeRange{
		long first,second;
		public TimeRange(long a,long b){
			first=a;
			second=b;
		}
	}

	public class Returns{
		int mid_point;
        int src_child; 
        int tar_father;
        long mid_point_arrival; 
        long mid_point_departure;
        int first_bus; 
        int second_bus;
	}
	
	TimeRange earliest_arrival_query(int src, int tar, long timerange_a, long timerange_b){
        src = leveltovertexid[src];
        tar = leveltovertexid[tar];
        TimeRange earliest_arrival=new TimeRange(timerange_a, MAX_TIMESTAMP);
        int x,y;

        for(int i=0, j=0; i<label0.get(src).m_num && j<label1.get(tar).m_num;){
            VectorLabelQunituple outlabel_list = label0.get(src).get(i);
            x = outlabel_list.get(0).v;
            VectorLabelQunituple inlabel_list = label1.get(tar).get(j);
            y = inlabel_list.get(0).v;

            if( x < y) i++;
            else if( x > y) j++;
            else{
                //cout<<"fuck!!!"<<endl;
                long outlabel_ta, outlabel_tb;
                long inlabel_ta, inlabel_tb;
                //int k=0, m=0;
                int k = Search_Label_Qunituple(timerange_a, outlabel_list);
                int m = Search_Label_Qunituple(timerange_a, inlabel_list);
                finished:
                for(; k<outlabel_list.m_num && m< inlabel_list.m_num;){
                    outlabel_ta = outlabel_list.get(k).ta;
                    outlabel_tb = outlabel_list.get(k).tb;
                    inlabel_ta = inlabel_list.get(m).ta;
                    inlabel_tb = inlabel_list.get(m).tb;
                    if(outlabel_ta < timerange_a){k++; continue;}
                    if(inlabel_ta < timerange_a){ m++; continue;}
                    if(inlabel_tb > timerange_b){
                        //i++; j++;
                        break finished;
                        //break;
                    }
                    if(outlabel_tb <= inlabel_ta ){
                    	if(earliest_arrival.second >= inlabel_tb){
                        ///if(earliest_arrival.second > inlabel_tb){
                    		if(earliest_arrival.first>=outlabel_ta){//����if��==��ʱ��ʾ�ҵ�����ʱ�����һ���·����ȡ��һ��
                    			k++;
                    			continue;
                    		}
                            earliest_arrival.first = outlabel_ta;
                            earliest_arrival.second = inlabel_tb;
                        }
                    	//�û����Ի�������Ŀ�ĵ�ʱ����ͬ������£�
                        break finished;//�����ϳ�
                        ///k++;//�������ϳ�
                    }else m++;
                }

                i++;
                j++;

            }
        }
        return earliest_arrival;

    }
	
	TimeRange earliest_arrival_query(int src, int tar, long timerange_a, long timerange_b,Returns re,ttr sca, ttr scb){
		
		int mid_point=re.mid_point;
        int src_child=re.src_child; 
        int tar_father=re.tar_father;
        long mid_point_arrival=re.mid_point_arrival; 
        long mid_point_departure=re.mid_point_departure;
        int first_bus=re.first_bus; 
        int second_bus=re.second_bus;
		TimeRange earliest_arrival=new TimeRange(timerange_a, MAX_TIMESTAMP);
		int x,y;

		for(int i=0, j=0; i<label0.get(src).m_num && j<label1.get(tar).m_num;){
			VectorLabelQunituple outlabel_list = label0.get(src).get(i);
			x = outlabel_list.get(0).v;
			VectorLabelQunituple inlabel_list = label1.get(tar).get(j);
			y = inlabel_list.get(0).v;

			if( x < y) i++;
			else if( x > y) j++;
			else{
				//cout<<"fuck!!!"<<endl;
				long outlabel_ta, outlabel_tb;
				long inlabel_ta, inlabel_tb;
				int walk_time=0;
				int bus1,bus2;
				//int k=0, m=0;
				int k = Search_Label_Qunituple(timerange_a, outlabel_list);
				int m = Search_Label_Qunituple(timerange_a, inlabel_list);
				finished:
				for(; k<outlabel_list.m_num && m< inlabel_list.m_num;){
					outlabel_ta = outlabel_list.get(k).ta;
					outlabel_tb = outlabel_list.get(k).tb;
					inlabel_ta = inlabel_list.get(m).ta;
					inlabel_tb = inlabel_list.get(m).tb;
					///
					bus1= outlabel_list.get(k).endOrStartBus;
					bus2=inlabel_list.get(m).endOrStartBus;
					if(bus1!=bus2)
						walk_time=3;
					else
						walk_time=0;
					///
					if(outlabel_ta < timerange_a){k++; continue;}
					if(inlabel_ta < timerange_a){ m++; continue;}
					if(inlabel_tb > timerange_b){
						//i++; j++;
						break finished;
						//break;
					}
					if(outlabel_tb +walk_time<= inlabel_ta ){
//						if(inlabel_tb>earliest_arrival.second)
//							{k++;}
						///if(earliest_arrival.second > inlabel_tb){
						if(earliest_arrival.second >= inlabel_tb){
							if(earliest_arrival.second == inlabel_tb&&earliest_arrival.first>=outlabel_ta){//��ͬ�ĵ���ʱ�䣬ȡ����ʱ�������·��������ʱ�������ȵ�·��ʱ��ȡ��һ��
                    			k++;
                    			continue;
                    		}
							earliest_arrival.first = outlabel_ta;
							earliest_arrival.second = inlabel_tb;
							mid_point = x;
							mid_point_arrival = outlabel_tb;
							mid_point_departure = inlabel_ta;
							src_child = outlabel_list.get(k).w;
							tar_father = inlabel_list.get(m).w;
							first_bus = outlabel_list.get(k).busid;
							second_bus = inlabel_list.get(m).busid;
							sca.lp = outlabel_list.get(k).iw;
							sca.rp = outlabel_list.get(k).wv;
							scb.lp = inlabel_list.get(m).iw;
							scb.rp = inlabel_list.get(m).wv;
						}
						break finished;///�����ϳ�
						///k++;
						//m++;
					}else m++;
				}
				i++;
				j++;

			}
		}
		
		re.mid_point =mid_point;
        re.src_child =src_child; 
        re.tar_father = tar_father;
        re.mid_point_arrival = mid_point_arrival; 
        re.mid_point_departure  = mid_point_departure;
        re.first_bus =first_bus; 
        re.second_bus = second_bus;
		return earliest_arrival;

	}
	
	
	Vector<ttr> earliest_arrival_path_query(int src, int tar, long timerange_a, long timerange_b){
        Vector<ttr> path=new Vector<ttr>();
        Stack<ttr> shortcut_stack =new Stack<ttr>();
        if(src == tar){
            return path;
        }
        if(src<0||src>N||tar<0||tar>N)
        	return path;
        src = leveltovertexid[src];
        tar = leveltovertexid[tar];
        ttr lh_shortcut=new ttr();
        ttr rh_shortcut=new ttr();
        lh_shortcut.father = -1;
        rh_shortcut.father = -1;
        lh_shortcut.lp = null;
        lh_shortcut.rp = null;
        rh_shortcut.lp = null;
        rh_shortcut.rp = null;
        int mid_point=-1;
        Returns re=new Returns();
        re.mid_point =mid_point;
        re.src_child =lh_shortcut.father; 
        re.tar_father = rh_shortcut.father;
        re.mid_point_arrival = lh_shortcut.arrival_time; 
        re.mid_point_departure  = rh_shortcut.depar_time;
        re.first_bus =lh_shortcut.busid; 
        re.second_bus = rh_shortcut.busid;
        
        TimeRange duration = earliest_arrival_query(src, tar, timerange_a, timerange_b,re,lh_shortcut, rh_shortcut );
        if(duration.second == MAX_TIMESTAMP) return path;
        mid_point=re.mid_point;
        lh_shortcut.father=re.src_child; 
        rh_shortcut.father=re.tar_father;
        lh_shortcut.arrival_time=re.mid_point_arrival; 
        rh_shortcut.depar_time=re.mid_point_departure;
        lh_shortcut.busid=re.first_bus; 
        rh_shortcut.busid=re.second_bus;
        assert(mid_point!=-1);
        lh_shortcut.u = src; lh_shortcut.v = mid_point; lh_shortcut.depar_time = duration.first;
        rh_shortcut.u = mid_point; rh_shortcut.v = tar; rh_shortcut.arrival_time = duration.second;
        shortcut_stack.push(rh_shortcut);
        shortcut_stack.push(lh_shortcut);
        while(!shortcut_stack.empty()){
            ttr top_shortcut = shortcut_stack.peek();
            shortcut_stack.pop();
            if(top_shortcut.u == top_shortcut.v|| top_shortcut.u == top_shortcut.father ||
               top_shortcut.v == top_shortcut.father){
                continue;
            }
            else if( top_shortcut.father <0){
            	top_shortcut.u = vertexid2level[top_shortcut.u];
                top_shortcut.v = vertexid2level[top_shortcut.v];
          	    path.add(top_shortcut);
                
            }else{
               ttr lh_shortcut2=new ttr();
               ttr rh_shortcut2=new ttr();
               lh_shortcut2.u = top_shortcut.u;
               lh_shortcut2.depar_time = top_shortcut.depar_time;
               lh_shortcut2.arrival_time = -1;
               lh_shortcut2.v = top_shortcut.father;
               lh_shortcut2.father = -1;
               lh_shortcut2.lp = null;
               lh_shortcut2.rp = null;

               rh_shortcut2.u = top_shortcut.father;
               rh_shortcut2.v = top_shortcut.v;
               rh_shortcut2.arrival_time = top_shortcut.arrival_time;
               rh_shortcut2.father = -1;
               rh_shortcut2.depar_time = -1;
               rh_shortcut2.lp = null;
               rh_shortcut2.rp = null;

               if(top_shortcut.rp != null){///rp==null?????
                   LabelQunituple sc2 = top_shortcut.rp;
                   rh_shortcut2.father = sc2.w;
                   ///rh_shortcut2.depar_time = sc2.tb;
                   rh_shortcut2.depar_time = sc2.ta;
                   rh_shortcut2.busid = sc2.busid;
                   rh_shortcut2.lp = sc2.iw;
                   rh_shortcut2.rp = sc2.wv;
                   shortcut_stack.push(rh_shortcut2);
               }

               if(top_shortcut.lp != null){
                   LabelQunituple sc1 = top_shortcut.lp;
                   lh_shortcut2.father = sc1.w;
                   lh_shortcut2.arrival_time = sc1.tb;
                   lh_shortcut2.busid = sc1.busid;
                   lh_shortcut2.lp = sc1.iw;
                   lh_shortcut2.rp = sc1.wv;
                   shortcut_stack.push(lh_shortcut2);
               }
            }
        }
        return path;
    }
	
	
	Vector<ttr> earliest_arrival_concise_path_query(int src, int tar, long timerange_a, long timerange_b){
        Vector<ttr> path=new Vector<ttr>();
        Stack<ttr> shortcut_stack =new Stack<ttr>();
        if(src == tar){
            return path;
        }
        if(src<0||src>N||tar<0||tar>N)
        	return path;
        src = leveltovertexid[src];
        tar = leveltovertexid[tar];
        ttr lh_shortcut=new ttr();
        ttr rh_shortcut=new ttr();
        lh_shortcut.father = -1;
        rh_shortcut.father = -1;
        lh_shortcut.lp = null;
        lh_shortcut.rp = null;
        rh_shortcut.lp = null;
        rh_shortcut.rp = null;
        int mid_point=-1;
        Returns re=new Returns();
        re.mid_point =mid_point;
        re.src_child =lh_shortcut.father; 
        re.tar_father = rh_shortcut.father;
        re.mid_point_arrival = lh_shortcut.arrival_time; 
        re.mid_point_departure  = rh_shortcut.depar_time;
        re.first_bus =lh_shortcut.busid; 
        re.second_bus = rh_shortcut.busid;
        
        TimeRange duration = earliest_arrival_query(src, tar, timerange_a, timerange_b,re,lh_shortcut, rh_shortcut );
        if(duration.second == MAX_TIMESTAMP) return path;
        mid_point=re.mid_point;
        lh_shortcut.father=re.src_child; 
        rh_shortcut.father=re.tar_father;
        lh_shortcut.arrival_time=re.mid_point_arrival; 
        rh_shortcut.depar_time=re.mid_point_departure;
        lh_shortcut.busid=re.first_bus; 
        rh_shortcut.busid=re.second_bus;
        assert(mid_point!=-1);
        lh_shortcut.u = src; lh_shortcut.v = mid_point; lh_shortcut.depar_time = duration.first;
        rh_shortcut.u = mid_point; rh_shortcut.v = tar; rh_shortcut.arrival_time = duration.second;
        shortcut_stack.push(rh_shortcut);
        shortcut_stack.push(lh_shortcut);

        int preBus=-2;
        while(!shortcut_stack.empty()){
            ttr top_shortcut = shortcut_stack.peek();
            shortcut_stack.pop();
            if(top_shortcut.u == top_shortcut.v|| top_shortcut.u == top_shortcut.father ||
               top_shortcut.v == top_shortcut.father){
                continue;
            }
            else if( top_shortcut.father <0||top_shortcut.busid!=-1){//top_shortcut.busid!=-1��ʾֱ��·������չ����
            	top_shortcut.u = vertexid2level[top_shortcut.u];
                top_shortcut.v = vertexid2level[top_shortcut.v];
            	if(top_shortcut.busid==preBus){
                	path.lastElement().arrival_time=top_shortcut.arrival_time;
                	path.lastElement().v=top_shortcut.v;
                }else{
                	path.add(top_shortcut);
                	preBus=top_shortcut.busid;
                }
            }else if(top_shortcut.busid==-1){
               ttr lh_shortcut2=new ttr();
               ttr rh_shortcut2=new ttr();
               lh_shortcut2.u = top_shortcut.u;
               lh_shortcut2.depar_time = top_shortcut.depar_time;
               lh_shortcut2.arrival_time = -1;
               lh_shortcut2.v = top_shortcut.father;
               lh_shortcut2.father = -1;
               lh_shortcut2.lp = null;
               lh_shortcut2.rp = null;

               rh_shortcut2.u = top_shortcut.father;
               rh_shortcut2.v = top_shortcut.v;
               rh_shortcut2.arrival_time = top_shortcut.arrival_time;
               rh_shortcut2.father = -1;
               rh_shortcut2.depar_time = -1;
               rh_shortcut2.lp = null;
               rh_shortcut2.rp = null;

               if(top_shortcut.rp != null){
                   LabelQunituple sc2 = top_shortcut.rp;
                   rh_shortcut2.father = sc2.w;
                   ///rh_shortcut2.depar_time = sc2.tb;
                   rh_shortcut2.depar_time = sc2.ta;
                   rh_shortcut2.busid = sc2.busid;
                   rh_shortcut2.lp = sc2.iw;
                   rh_shortcut2.rp = sc2.wv;
                   shortcut_stack.push(rh_shortcut2);
               }

               if(top_shortcut.lp != null){
                   LabelQunituple sc1 = top_shortcut.lp;
                   lh_shortcut2.father = sc1.w;
                   lh_shortcut2.arrival_time = sc1.tb;
                   lh_shortcut2.busid = sc1.busid;
                   lh_shortcut2.lp = sc1.iw;
                   lh_shortcut2.rp = sc1.wv;
                   shortcut_stack.push(lh_shortcut2);
               }
            }
        }
        return path;
    }
	
	public void output(String index_file_str) throws Exception{
        //cout<<"output index files"<<endl;
        String order_file_str=index_file_str+".order";
        String num=index_file_str+"JAVA.num";
        index_file_str+="JAVA.index";
        System.out.println("Index file: "+index_file_str);
        //cout<<"Order file: "<<order_file_str<<endl;	
       // FILE* index_file = fopen(index_file_str.c_str(), "w");
       // OutputStream os = new FileOutputStream(index_file_str);
       // DataOutputStream dos = new DataOutputStream(os);
        BufferedWriter bw = new BufferedWriter(new FileWriter(index_file_str));
        BufferedWriter bw2 = new BufferedWriter(new FileWriter(num));
        //FILE* order_file = fopen(order_file_str.c_str(), "w");
        //fprintf(index_file, "%d\n", hybrid_order_start);
//        for(int i=0; i<N; i++){
//            fprintf(order_file, "%d\n", vertexid2level[i]);
//        }
		for(int i=0; i<N; i++){
			//fprintf(index_file, "%d:", i);
			bw.write(Integer.toString(i));
			bw.write(":");
			int num_labels=0;
			for(int j=0; j<label0.get(i).m_num; j++)
			{
				num_labels+=label0.get(i).get(j).m_num;
				for( int k=0; k<label0.get(i).get(j).m_num; k++){
					String v=Integer.toString(label0.get(i).get(j).get(k).v);
					String ta=Integer.toString((int) label0.get(i).get(j).get(k).ta);
					String dur=Integer.toString((int) (label0.get(i).get(j).get(k).tb-label0.get(i).get(j).get(k).ta));
					String father=Integer.toString(label0.get(i).get(j).get(k).father);
					String busid=Integer.toString(label0.get(i).get(j).get(k).busid);
					String s=v+" "+ta+" "+dur+" "+father+" "+busid+" ";
					bw.write(s);
				}
			}
			bw.write("lin: ");
			for(int j=0; j<label1.get(i).m_num; j++)
			{
				num_labels+=label1.get(i).get(j).m_num;
				for( int k=0; k<label1.get(i).get(j).m_num; k++)
				{
					String v=Integer.toString(label1.get(i).get(j).get(k).v);
					String ta=Integer.toString((int) label1.get(i).get(j).get(k).ta);
					String dur=Integer.toString((int) (label1.get(i).get(j).get(k).tb-label1.get(i).get(j).get(k).ta));
					String father=Integer.toString(label1.get(i).get(j).get(k).father);
					String busid=Integer.toString(label1.get(i).get(j).get(k).busid);
					String s=v+" "+ta+" "+dur+" "+father+" "+busid+" ";
					bw.write(s);
				}
			}
			bw.write( "-2\n");
			bw2.write(Integer.toString(i));
			bw2.write(":");
			bw2.write(Integer.toString(num_labels));
			bw2.write("\n");
			num_labels=0;
		}
		bw.flush();
		bw.close();
		bw2.flush();
		bw2.close();
		System.out.println("output index complete");
       // fclose(order_file);
    }

	///δ����޸�����ʽ�Ż�ƫ��
//	void compute_index_order_coverage_heuristic(String fn){
//        int ql,i,j,k,m,u,v,w,tu,tv,fu,s;
//		char starting[]=new char[N];
//		
//        tri fi;
//		//memset(starting,0,N);
//        Random rd=new Random();
//	
//			//printf("file \"%s\" not found, all nodes will be sampled.\n",fn.c_str());
//		for(i=0;i<N;i++)
//			starting[i]=1;
//        //puts("constructing trees...");
//        m=0;
//        for(i=0;i<links0.m_num;i++)
//            m+=links0.get(i).m_num;
//        s=m*4;
//        fa=new Vector<Vector<tri>>(N);
//        ch=new Vector<Vector<tri>>(N);
//        //fa.resize(N);
//        //ch.resize(N);
//        p=new int[N+1];
//        for(i=0;i<N;i++)
//        {
//            p[i]=i;
//            fa.get(i).clear();
//            ch.get(i).clear();
//        }
//        for(i=N-1;i!=0;i--)
//        {
//            j= rd.nextInt()%(i+1);//(unsigned int((rand()<<15)+rand()))%(i+1);
//            k=p[i];
//            p[i]=p[j];
//            p[j]=k;
//        }
//        tb=new int[N];
//        q=new int[m];
//        fq=new int[m];
//        tq=new int[m];
//        for(i=0;i<N;i++)
//        	tb[i]=-1;
//       // memset(tb,-1,sizeof(int)*N);
//		m=0;
//        for(i=0;i<N;i++)
//        {
//            if(s<1)break;
//            v=p[i];
//			if(starting[v]==0)continue;
//            //printf("%d nodes sampled\t\t%d space remainning...%c",m++,s,13);
//            Vector<Integer>tbs;
//            tbs.clear();
//            tbs.add(v);
//			Map<Integer,Integer>tta=new HashMap<Integer,Integer>();
//			tta.clear();
//			int r=stamps;
//			for(j=links0.get(v).m_num-1;j>=0;j--)
//				if(rd.nextInt()%(j+1)<r)
//				{
//					tta.put((int) links0.get(v).get(j).ta, 1);
//					//tta[links[0][v][j].ta]=1;
//					r--;
//				}
//            for(j=links0.get(v).m_num-1;j>=0;)
//            {
//                tv=(int) links0.get(v).get(j).ta;
//                
//				if(stamps>0&&tta.containsKey(tv))
//				{
//					j--;
//					continue;
//				}
//				//printf("sampled %d %d %c",i,tv,13);
//                tb[v]=tv;
//                fi.v=v;
//                fi.t=tv;
//                fi.w=v;
//                fa.get(v).add(fi);
//                ql=0;
//                while(j>=0&&links0.get(v).get(j).ta==tv)
//                {
//                    add_q(ql,links0.get(v).get(j).v,v,(int)links0.get(v).get(j).tb);
//                    ql++;
//                    j--;
//                }
//                while(ql!=0)
//                {
//                    u=q[0];
//                    fu=fq[0];
//                    tu=tq[0];
//                    del_q(ql);
//                    ql--;
//                    if(tb[u]>=0&&tu>=tb[u])continue;
//                    if(tb[u]<0)tbs.add(u);
//                    tb[u]=tu;
//                    fi.w=fu;
//                    fa.get(u).add(fi);
//                    fi.w=u;
//                    ch.get(fu).add(fi);
//                    s--;
//                    for(k=0;k<links0.get(u).m_num;k++)
//                    {
//                        w=links0.get(u).get(k).v;
//                        if(links0.get(u).get(k).ta>=tu&&(tb[w]<0||links0.get(u).get(k).tb<tb[w]))add_q(ql,w,u,(int)links0.get(u).get(k).tb);
//                    }
//                }
//            }
//            for(j=0;j<tbs.size();j++)
//                tb[tbs.get(j)]=-1;
//        }
//        p[i]=-1;
//        //puts("hashing the tree edges...");
//        chv.clear();
//        int as=0;
//        for(i=0;i<N;i++)
//        {
//            //printf("%d %c",i,13);
//            Vector<tri>temp=fa.get(i);
//            q[i]=temp.size();
//            for(fq[i]=1;fq[i]*2/3<q[i];fq[i]=fq[i]*2+1);
//            fa.get(i).resize(fq[i]);
//            for(j=0;j<fq[i];j++)
//                fa[i][j].w=-1;
//            for(j=0;j<temp.size();j++)
//            {
//                for(k=hash_fun(temp[j].v,temp[j].t)%fq[i];fa[i][k].w>=0;k=((k>fq[i]-2)?0:k+1),as++);
//                fa[i][k]=temp[j];
//            }
//            temp=ch[i];
//            ch[i].clear();
//            v=-1;
//            for(j=0;j<temp.size();j++)
//            {
//                if(temp[j].v!=v||temp[j].t!=tv)
//                {
//                    chv.push_back(-1);
//                    ch[i].push_back(temp[j]);
//                    ch[i][ch[i].size()-1].w=chv.size();
//                    v=temp[j].v;
//                    tv=temp[j].t;
//                }
//                chv.push_back(temp[j].w);
//            }
//            temp=ch[i];
//            for(tq[i]=1;tq[i]*2/3<temp.size();tq[i]=tq[i]*2+1);
//            ch[i].resize(tq[i]);
//            for(j=0;j<tq[i];j++)
//                ch[i][j].w=-1;
//            for(j=0;j<temp.size();j++)
//            {
//                for(k=hash_fun(temp[j].v,temp[j].t)%tq[i];ch[i][k].w>=0;k=((k>tq[i]-2)?0:k+1),as++);
//                ch[i][k]=temp[j];
//            }
//        }
//        chv.push_back(-1);
//        //puts("calculating coverage...");
//        cv.resize(N);
//        for(i=0;i<N;i++)
//            cv[i].clear();
//        for(i=0;p[i]>=0;i++)
//        {
//            //printf("%d %c",i,13);
//            v=p[i];
//			if(!starting[v])continue;
//            for(j=links[0][v].m_num-1;j>=0;)
//            {
//                for(tv=links[0][v][j].ta;j>=0&&links[0][v][j].ta==tv;j--);
//                add_cvt(v,v,tv);
//            }
//        }
//        //puts("ordering...");
//        memset(p,0,sizeof(int)*N);
//        for(i=0;i<N;i++)
//        {
//            //printf("%d %c",i,13);
//            double max=-1;
//            for(j=0;j<N;j++)
//                if(!p[j])
//                {
//                    double cvs=(in_deg[j]+1)*(out_deg[j]+1)/1e9;
//                    for(k=0;k<cv[j].size();k++)
//                        cvs+=cv[j][k].second;
//                    if(cvs>max)
//                    {
//                        max=cvs;
//                        u=j;
//                    }
//                }
//            vertexid2level[i]=u;
//            p[u]=1;
//            while(fq[u]>1)
//            {
//                while(1)
//                {
//                    j = rand()%fq[u];
//                    //j=(unsigned int((rand()<<15)+rand()))%fq[u];
//                    v=fa[u][j].v;
//                    tv=fa[u][j].t;
//                    w=fa[u][j].w;
//                    if(w>=0)break;
//                }
//                j=del_tr(u,v,tv);
//                if(w==u)continue;
//                k=u;
//                while(w!=k)
//                {
//                    add_cv(w,v,-j);
//                    k=w;
//                    w=get_fa(k,v,tv);
//                }
//            }
//        }
//        end_time = clock();
//        ordering_time = (double)(end_time-start_time)/CLOCKS_PER_SEC ;
//        cout<<"Ordering time: "<<ordering_time<<endl;
//        //printf( "Ordering time: %0.2lf\n",(double)(end_time-start_time)/CLOCKS_PER_SEC );
//        delete []in_deg;
//        delete []out_deg;
//        delete []p;
//        delete []tb;
//        delete []q;
//        delete []fq;
//        delete []tq;
//    }
//	
//	void add_q(int ql,int v,int f,int t)
//    {
//        int i,j,k;
//        q[ql]=v;
//        fq[ql]=f;
//        tq[ql]=t;
//        i=ql++;
//        while(i!=0)
//        {
//            j=(i-1)/2;
//            if(tq[i]<tq[j])
//            {
//                k=q[i];
//                q[i]=q[j];
//                q[j]=k;
//                k=fq[i];
//                fq[i]=fq[j];
//                fq[j]=k;
//                k=tq[i];
//                tq[i]=tq[j];
//                tq[j]=k;
//            }
//            else break;
//            i=j;
//        }
//    }
//
//    void del_q(int ql)
//    {
//        int i,j,k;
//        ql--;
//        q[0]=q[ql];
//        fq[0]=fq[ql];
//        tq[0]=tq[ql];
//        i=0;
//        while(1)
//        {
//            j=i*2+1;
//            if(j>=ql)break;
//            if(j+1<ql&&tq[j+1]<tq[j])j++;
//            if(tq[j]<tq[i])
//            {
//                k=q[i];
//                q[i]=q[j];
//                q[j]=k;
//                k=fq[i];
//                fq[i]=fq[j];
//                fq[j]=k;
//                k=tq[i];
//                tq[i]=tq[j];
//                tq[j]=k;
//            }
//            else break;
//            i=j;
//        }
//    }
//
//    int add_cvt(int u,int v,int t)
//        {
//            int s=1;
//            for(int i=get_ch(u,v,t);chv[i]>=0;i++)
//                s+=add_cvt(chv[i],v,t);
//            add_cv(u,v,s);
//            return s;
//        }
//    void add_cv(int u,int v,int s)
//        {
//            int i;
//            for(i=0;i<cv[u].size();i++)
//                if(cv[u][i].first==v)
//                {
//                    cv[u][i].second+=s;
//                    return;
//                }
//            cv[u].push_back(make_pair(v,s));
//        }
//
//    int get_ch(int u,int v,int t)
//        {
//            int r;
//            r=hash_fun(v,t)%tq[u];
//            while(1)
//            {
//                if(ch[u][r].w<0)return 0;
//                if(ch[u][r].v==v&&ch[u][r].t==t)return ch[u][r].w;
//                r++;
//                if(r==tq[u])r=0;
//            }
//        }
//
	    	 
	    
}
