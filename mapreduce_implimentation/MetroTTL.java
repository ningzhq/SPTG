package com.cloudyhadoop.hadoop.mapreduce;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.Stack;
import java.util.Vector;
import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;








import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.cloudyhadoop.hadoop.mapreduce.EdgeQuintuple; 
import com.cloudyhadoop.hadoop.mapreduce.HHL_temporal2.CombineMapper;
import com.cloudyhadoop.hadoop.mapreduce.HHL_temporal2.CombineMapper2;
import com.cloudyhadoop.hadoop.mapreduce.HHL_temporal2.MyMapper;
import com.cloudyhadoop.hadoop.mapreduce.HHL_temporal2.MyMapper2;
import com.cloudyhadoop.hadoop.mapreduce.HHL_temporal2;






import com.cloudyhadoop.hadoop.mapreduce.HHL_temporal2.MyReducer;
import com.cloudyhadoop.hadoop.mapreduce.HHL_temporal2.MyReducer2;

import java.util.PriorityQueue;
import java.util.Random;

public class MetroTTL {
		
	long MAX_TIMESTAMP = Long.MAX_VALUE - 3;
	final int VectorDefaultSize = 20;

	class Key_Value implements Comparable {
		int key;
		long value;
		///
		int bus;
		///
		Key_Value(int k, long v,int b) {
			key = k;
			value = v;
			bus =b;
		}

		
		public int compareTo(Object o) {
			Key_Value p = (Key_Value) o;
			if (key == p.key)
				return 0;
			if (key < p.key)
				return 1;
			else if (key > p.key)
				return -1;
			if (value < p.value)
				return 1;
			else
				return -1;
		}
		// bool operator==( const Key_Value<_Key,_Value>& p )const
		// {
		// return ( key == p.key );
		//
		// }
		// bool operator<( const Key_Value<_Key,_Value>& p )const
		// {
		// if ( key < p.key ) return true;
		// else if ( key > p.key ) return false;
		// return ( value < p.value );
		// }

		Key_Value(int tmp) {
			key = tmp;
			value = -1;
		}

		Key_Value() {
		}
	}

	public class iVector<_T extends Comparable> implements Comparable {
		public int m_size;
		ArrayList<_T> m_data;

		int m_num;

		void free_mem() {
			m_data.clear();
			;
		}

		
		public int compareTo(Object o) {
			return 1;
		}

		public iVector() {
			// printf("%d\n",VectorDefaultSize);
			m_size = VectorDefaultSize;
			m_data = new ArrayList<_T>(m_size);
			m_num = 0;
		}

		void re_allocate(int size) {
			if (size < m_num) {
				return;
			}
			m_data = new ArrayList<_T>(size);
			m_size = size;
		}

		// public iVector( int n )
		// {
		// if ( n == 0 )
		// {
		// n = VectorDefaultSize;
		// }
		//// printf("iVector allocate: %d\n",n);
		// m_size = n;
		// //m_data = (_T[]) new Object[m_size];
		// m_num = 0;
		// }
		void push_back(_T d) {
			// if ( m_num == m_size )
			// {
			// re_allocate( m_size*2 );
			// }
			// m_data[m_num] = d ;
			m_data.add(d);
			m_num++;
		}

		void sorted_insert(_T x) {

			if (m_num == 0) {
				push_back(x);
				return;
			}
			push_back(x);
			Collections.sort(m_data, new Comparator() {

				public int compare(Object arg0, Object arg1) {
					// TODO Auto-generated method stub
					_T dept1 = (_T) arg0;
					_T dept2 = (_T) arg1;
					return dept1.compareTo(dept2);
				}

			});

			// if ( m_num == m_size ) re_allocate( m_size*2 );

			// int l,r;
			//
			// for ( l = 0 , r = m_num ; l < r ; )
			// {
			// int m = (l+r)/2;
			// if ( m_data.get(m).compareTo(x) < 0 ) l = m+1;
			// else r = m;
			// }
			//
			// if ( l < m_num && m_data.get(l).equals(x) )
			// {
			// //printf("Insert Duplicate....\n");
			// //cout<<x<<endl;
			// // break;
			// }
			// else
			// {
			// //Arrays.copyOfRange(m_data, from, to)
			// if ( m_num > l )
			// {
			//
			// m_data.get(l+i)=m_data.get(l+i+1)
			//
			//
			// memmove( m_data+l+1, m_data+l, sizeof(_T)*(m_num-l) );
			// }
			// m_num++;
			// m_data[l] = x;
			// }
		}

		// boolean remove_unsorted( _T x )
		// {
		// for ( int m = 0 ; m < m_num ; ++m )
		// {
		// if ( m_data[m] == x )
		// {
		// m_num--;
		// if ( m_num > m ) memcpy( m_data+m, m_data+m+1, sizeof(_T)*(m_num-m)
		// );
		// return true;
		// }
		// }
		// return false;
		// }

		_T get(int i) {
			// if ( i < 0 || i >= m_num )
			// {
			// printf("iVector [] out of range!!!\n");
			// }
			return m_data.get(i);
		}

		void clean() {
			m_num = 0;
		}
		// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		// close range check for [] in iVector if release

	}

	class iMap {
		long m_data[];
		int m_num;
		int cur;
		VectorInteger occur =new VectorInteger();
		int nil;

		iMap() {
			 m_data = null;
			m_num = 0;
			// nil = std::make_pair((long)-9,(long)-9);
			// nil = 1073741834;
		}

//		iMap(_T tt) {
//			// m_data = NULL;
//			m_num = 0;
//			nil = (int) tt;
//		}

//		void free_mem() {
//			m_data.clear();
//			occur.free_mem();
//		}

		void initialize(int n) {
			occur.clean();
			m_num = n;
			nil = -9;
			// if ( !m_data.isEmpty() )
			// m_data.clear();;
			m_data = new long[m_num];
			for ( int i = 0 ; i < m_num ; ++i )
				m_data[i] = nil;
			cur = 0;
		}

		void clean() {
			for (int i = 0; i < occur.m_num; ++i) {
				m_data[occur.get(i)] = nil;

			}
			occur.clean();
			cur = 0;
		}

		long get(int p) {
			// if ( p < 0 || p >= m_num )
			// {
			// printf("iMap get out of range!!!\n");
			// return -8;
			// }
			return m_data[p];
		}

		// int get( int p )
		// {
		// //if ( i < 0 || i >= m_num )
		// //{
		// // printf("iVector [] out of range!!!\n");
		// //}
		// return m_data.get(p)
		// }
		void erase(int p) {
			// if ( p < 0 || p >= m_num )
			// {
			// printf("iMap get out of range!!!\n");
			// }
			m_data[p] = nil;
			cur--;
		}

		boolean notexist(int p) {
			return m_data[p] == nil ;
			// return m_data[p] == nil ;
		}

		boolean exist(int p) {
			return !(m_data[p] == nil);
		}

		void insert(int p, int d) {
			// if ( p < 0 || p >= m_num )
			// {
			// printf("iMap insert out of range!!!\n");
			// }
			if ( m_data[p] == nil )
			{
				occur.push_back( p );
				cur++;
			}
			m_data[p] = d;
		}

	}

	class TimeBus{///��¼���ﶥ��ʱ��ʱ��������ĳ���
		long time;
		int bus;
	}
	class iMap2 {
		TimeBus m_data[];//�൱��dijkstra�еĸ�������,���ϱ��ɳ�
		int m_num;
		int cur;
		VectorInteger occur =new VectorInteger();
		int nil;

		iMap2() {
			 m_data = null;
			m_num = 0;

		}



		void initialize(int n) {
			occur.clean();
			m_num = n;
			nil = -9;

			m_data = new TimeBus[m_num];
			for ( int i = 0 ; i < m_num ; ++i )
			{
				m_data[i].time = nil;
				m_data[i].bus=nil;
			}
			cur = 0;
		}

		void clean() {
			for (int i = 0; i < occur.m_num; ++i) {
				m_data[occur.get(i)].time = nil;
				m_data[occur.get(i)].bus = nil;

			}
			occur.clean();
			cur = 0;
		}

		long getTime(int p) {

			return m_data[p].time;
		}
		long getBus(int p) {

			return m_data[p].bus;
		}

		void erase(int p) {

			m_data[p].time = nil;
			m_data[p].bus = nil;
			cur--;
		}

		boolean notexist(int p) {
			return m_data[p].time == nil ;
			// return m_data[p] == nil ;
		}

		boolean exist(int p) {
			return !(m_data[p].time == nil);
		}

		void insert(int p, int d,int bus) {

			if ( m_data[p].time == nil )
			{
				occur.push_back( p );
				cur++;
			}
			m_data[p].time = d;
			m_data[p].bus=bus;
		}

	}
	
	
	
	class iHeap {
		iMap pos;
		VectorKeyValue m_data;
		public iHeap(){
			pos=new iMap();
			m_data=new VectorKeyValue();
		}

		void initialize(int n) {
			pos.initialize(n);

			//m_data = new VectorKeyValue();
			m_data.re_allocate(1000);

		}

		int head() {
			return m_data.m_data[0].key;
		}
		int bus(){
			return m_data.m_data[0].bus;
		}
		void clean() {
			pos.occur.clean();
		}

		void free_mem() {
			//pos.free_mem();
			//m_data.free_mem();
		}

		void DeepClean() {
			pos.clean();
			m_data.clean();
		}

		void insert(int x, long y,int bus) {///�����¶���bus
			if (pos.notexist(x)) {
				m_data.push_back(new Key_Value(x, y,bus));
				pos.insert(x, m_data.m_num - 1);
				up(m_data.m_num - 1);
			} else {
				if (y < m_data.m_data[(int) pos.get(x)].value) {
					m_data.m_data[(int) pos.get(x)].value = y;
					///
					m_data.m_data[(int) pos.get(x)].bus = bus;
					///
					up((int) pos.get(x));
				} else {
					///���ظ��ĵ���x�ĵ���ʱ��yһ���·���������ˣ���busû���⸳ֵ
					m_data.m_data[(int) pos.get(x)].value = y;
					down((int) pos.get(x));
				}
			}
		}

		int pop() {
			int tmp = m_data.m_data[0].key;
			pos.erase(tmp);
			if (m_data.m_num == 1) {
				m_data.m_num--;
				return tmp;
			}
			m_data.m_data[0] = m_data.m_data[m_data.m_num - 1];
			m_data.m_num--;
			down(0);
			return tmp;
		}

		boolean empty() {
			return (m_data.m_num == 0);
		}

		void up( int p )
	 {
	 Key_Value x = m_data.m_data[p];
	 for ( ; p > 0 && x.value < m_data.m_data[(p-1)/2].value ; p = (p-1)/2 )
	 {
	 m_data.m_data[p] = m_data.m_data[(p-1)/2];
	 pos.insert(m_data.m_data[p].key, p);
	 }
	 m_data.m_data[p] = x;
	 pos.insert(x.key,p);
	 }

		void down( int p )
	 {
	 //Key_Value<int,int> x = m_data[m_data.m_num-1];
	 //m_data.m_num--;
	
	 Key_Value tmp;
	
	 for ( int i ; p < m_data.m_num ; p = i )
	 {
	 //m_data[p] = x;
	 if ( p*2+1 < m_data.m_num && m_data.m_data[p*2+1].value < m_data.m_data[p].value )
	 i = p*2+1;
	 else i = p;
	 if ( p*2+2 < m_data.m_num && m_data.m_data[p*2+2].value < m_data.m_data[i].value )
	 i = p*2+2;
	 if ( i == p ) break;
	
	 tmp = m_data.m_data[p];
	 m_data.m_data[p] = m_data.m_data[i];
	 pos.insert( m_data.m_data[p].key, p );
	 m_data.m_data[i] = tmp;
	 }
	
	 pos.insert(m_data.m_data[p].key,p);
	 }

	}

	class tri {
		int v;
		int t;
		int w;
	};

	public class EdgeQuadruple implements Comparable {

		int v;
		long ta;
		long tb;
		int busid;

	
		public int compareTo(Object o) 
		{
			EdgeQuadruple e = (EdgeQuadruple) o;
			if (v == e.v && ta == e.ta && tb == e.tb && busid == e.busid)
				return 0;
			if (tb < e.tb)
				return -1;
			else if (tb > e.tb)
				return 1;
			else if (ta < e.ta)
				return -1;
			else if (ta > e.ta)
				return 1;
			else if (v < e.v)
				return -1;
			else if (v > e.v)
				return 1;
			else if (busid < e.busid)
				return -1;
			else
				return 1;
		
		
//		if (tb != e.tb)
//			return (int) (tb - e.tb);
//		else if (ta != e.ta)
//			return (int) (ta -e.ta);
//		else if (v != e.v)
//			return v - e.v;
//		else if (busid != e.busid)
//			return busid - e.busid;
//		else
//			return 0;

	}

		// bool operator==(const EdgeQuadruple &e)const
		// {
		// return v == e.v && ta == e.ta && tb == e.tb && busid == e.busid;
		// }
		EdgeQuadruple(int vv, long tta, long ttb, int bbusid) {
			v = vv;
			ta = tta;
			tb = ttb;
			busid = bbusid;
		}

		EdgeQuadruple() {
		}

		// public int compareTo(Object o) {
		// // TODO Auto-generated method stub
		// return 0;
		// }
	}

//	public class EdgeQuintuple implements Comparable {
//		int u;
//		int v;
//		long ta;
//		long tb;
//		int busid;
//
//		
//		public int compareTo(Object o) 
//		{
//			EdgeQuintuple e = (EdgeQuintuple) o;
//			if (u == e.u && v == e.v && ta == e.ta && tb == e.tb && busid == e.busid)
//				return 0;
//			if (ta < e.ta)
//				return 1;
//			else if (ta > e.ta)
//				return -1;
//			else if (tb < e.tb)
//				return 1;
//			else if (tb > e.tb)
//				return -1;
//			else if (u < e.u)
//				return 1;
//			else if (u > e.u)
//				return -1;
//			else if (v < e.v)
//				return 1;
//			else if (v > e.v)
//				return -1;
//			else if (busid < e.busid)
//				return 1;
//			else
//				return -1;
//		}
//
//		// bool operator<(const EdgeQuintuple &e) const
//		// {
//		// if ( ta < e.ta) return true;
//		// else if( ta > e.ta) return false;
//		// else if( tb < e.tb ) return true;
//		// else if( tb > e.tb ) return false;
//		// else if(u <e.u) return true;
//		// else if(u> e.u ) return false;
//		// else if(v < e.v) return true;
//		// else if(v > e.v) return false;
//		// else if(busid< e.busid) return true;
//		// else return false;
//		// }
//		// bool operator==(const EdgeQuintuple &e)const
//		// {
//		// return u == e.u && v == e.v && ta == e.ta && tb == e.tb && busid ==
//		// e.busid;
//		// }
//		EdgeQuintuple(int uu, int vv, long tta, long ttb) {
//			u = uu;
//			v = vv;
//			ta = tta;
//			tb = ttb;
//		}
//
//		EdgeQuintuple(int uu, int vv, long tta, long ttb, int bbusid) {
//			u = uu;
//			v = vv;
//			ta = tta;
//			tb = ttb;
//			busid = bbusid;
//		}
//
//		EdgeQuintuple() {
//		}
//	}

//	public class LabelQunituple implements Comparable {
//		int v;
//		long ta;
//		long tb;
//		int father;
//		int w;
//		int busid;
//		int compression_type;
//		int endOrStartBus;
//		LabelQunituple iw, wv;
//		iVector<LabelQunituple> list1, list2;
//
//	
//		public int compareTo(Object o) 
//		{
//			LabelQunituple e = (LabelQunituple) o;
//			if (v == e.v && ta == e.ta && tb == e.tb)
//				return 0;
//			if (v < e.v)
//				return 1;
//			else if (v > e.v)
//				return -1;
//			else if (ta < e.ta)
//				return 1;
//			else if (ta > e.ta)
//				return -1;
//			else if (tb < e.tb)
//				return 1;
//			else
//				return -1;
//		}
//
//		// bool operator<(const LabelQunituple &e) const
//		// {
//		// if(v <e.v) return true;
//		// else if (v > e.v ) return false;
//		// else if( ta < e.ta ) return true;
//		// else if ( ta > e.ta ) return false;
//		// else if(tb < e.tb ) return true;
//		// else return false;
//		// }
//		// bool operator==(const LabelQunituple &e)const
//		// {
//		// return v == e.v && ta == e.ta && tb == e.tb;
//		// }
//		LabelQunituple(int vv, long tta, long ttb, int ffather, int ww, int bbusid,int esbus) {
//			v = vv;
//			ta = tta;
//			tb = ttb;
//			father = ffather;
//			w = ww;
//			busid = bbusid;
//			endOrStartBus=esbus;
//			iw = null;
//			wv = null;
//			list1 = null;
//			list2 = null;
//			compression_type = 0;
//		}
//
//		LabelQunituple() {
//		}
//
//	}

//	public class ttr {
//		int u;
//		int v;
//		int father;
//		int busid;
//		long depar_time;
//		long arrival_time;
//		int compression_type;
//		LabelQunituple lp, rp;
//		VectorLabelQunituple llist, rlist;
//	}
	
	public class EdgeQuadrupleCmp implements Comparator{  
		  
	    public int compare(Object o1, Object o2) {  
	    	EdgeQuadruple t1=(EdgeQuadruple) o1;  
	    	EdgeQuadruple e=(EdgeQuadruple) o2;  
	    	if(t1.v == e.v && t1.ta == e.ta && t1.tb == e.tb && t1.busid == e.busid)
	    		return 0;
	        if ( t1.ta > e.ta) return 1;
	        else if( t1.ta < e.ta) return -1;
	        else if( t1.tb > e.tb ) return 1;
	        else if( t1.tb < e.tb ) return -1;
	        else  if(t1.v >e.v) return 1;///!!��compareTo()��ͬ
	        else  if(t1.v < e.v) return -1;
	        else if(t1.busid < e.busid) return 1;
	        else  return -1;  
	   }  
	     
	}  
	
	
	public class VectorEdgeQuadruple{
		public EdgeQuadruple m_data[]=new EdgeQuadruple[VectorDefaultSize];
		 int m_size=VectorDefaultSize;
		
		 int m_num=0;
		 VectorEdgeQuadruple()
		{
			//printf("%d\n",VectorDefaultSize);
			m_size = VectorDefaultSize;
			for(int i=0;i<VectorDefaultSize;i++)
				m_data[i] = new EdgeQuadruple();
			m_num = 0;
		}
		 VectorEdgeQuadruple(  int n )
		{
			if ( n == 0 )
			{
				n = VectorDefaultSize;
			}
//			printf("iVector allocate: %d\n",n);
			m_size = n;
			for(int i=0;i<n;i++)
				m_data[i] = new EdgeQuadruple();
			m_num = 0;
		}
		void push_back( EdgeQuadruple d )
		{
			if ( m_num == m_size )
			{
				re_allocate( m_size*2 );
			}
			m_data[m_num] = d ;
			m_num++;
		}
//		void push_back( const _T* p, unsigned int len )
//		{
//			while ( m_num + len > m_size )
//			{
//				re_allocate( m_size*2 );
//			}
//			memcpy( m_data+m_num, p, sizeof(_T)*len );
//			m_num += len;
//		}

		void re_allocate(  int size )
		{
			if ( size < m_num )
			{
				return;
			}
			EdgeQuadruple tmp[] = new EdgeQuadruple[size];
			for(int i=0;i<size;i++){
				tmp[i]=new EdgeQuadruple();
			}
			System.arraycopy(m_data,0,tmp,0,m_num);
			//memcpy( tmp, m_data, sizeof(_T)*m_num );
			m_size = size;
			//delete[] m_data;
			m_data=null;
			m_data = tmp;
		}

		void clean()
		{
			m_num = 0;
		}


		void sorted_insert( EdgeQuadruple x,int type )
		{
			if ( m_num == 0 )
			{
				push_back( x );
				return;
			}
			
			if ( m_num == m_size ) re_allocate( m_size*2 );
			push_back( x );
			//m_num++;
			if(type==1)
				Arrays.sort(m_data, 0, m_num, new EdgeQuadrupleCmp());
			else
				Arrays.sort(m_data,0,m_num);
			
//			int l,r;
//
//			for ( l = 0 , r = m_num ; l < r ; )
//			{
//				int m = (l+r)/2;
//				if ( m_data[m].compareTo(x)==1) l = m+1;
//				else r = m;
//			}
//
//			if ( l < m_num && m_data[l].compareTo(x)==0 )
//			{
//				//printf("Insert Duplicate....\n");
//	            //cout<<x<<endl;
//		//		break;
//			}
//			else
//			{
//				if ( m_num > l )
//				{
//					int k=m_num;
//					while(k>=(m_num-l)){
//						m_data[k]=m_data[k-1];
//						k--;
//					}
//					//System.arraycopy(m_data,0,tmp,0,m_num);
//					//memmove( m_data+l+1, m_data+l, sizeof(_T)*(m_num-l) );
//				}
//				m_num++;
//				m_data[l] = x;
//			}
		}
		
		EdgeQuadruple get(int i) {
			// if ( i < 0 || i >= m_num )
			// {
			// printf("iVector [] out of range!!!\n");
			// }
			return m_data[i];
		}

	}

	public class VectorInteger {
		public Integer m_data[] = new Integer[VectorDefaultSize];
		int m_size = VectorDefaultSize;

		int m_num = 0;

		VectorInteger() {
			// printf("%d\n",VectorDefaultSize);
			m_size = VectorDefaultSize;
			for (int i = 0; i < VectorDefaultSize; i++)
				m_data[i] = new Integer(0);
			m_num = 0;
		}

		VectorInteger(int n) {
			if (n == 0) {
				n = VectorDefaultSize;
			}
			// printf("iVector allocate: %d\n",n);
			m_size = n;
			for (int i = 0; i < n; i++)
				m_data[i] = new Integer(0);
			m_num = 0;
		}

		void push_back(Integer d) {
			if (m_num == m_size) {
				re_allocate(m_size * 2);
			}
			m_data[m_num] = d;
			m_num++;
		}
		// void push_back( const _T* p, unsigned int len )
		// {
		// while ( m_num + len > m_size )
		// {
		// re_allocate( m_size*2 );
		// }
		// memcpy( m_data+m_num, p, sizeof(_T)*len );
		// m_num += len;
		// }

		void re_allocate(int size) {
			if (size < m_num) {
				return;
			}
			Integer[] tmp = new Integer[size];
			for(int i=0;i<size;i++){
				tmp[i]=new Integer(0);
			}
			System.arraycopy(m_data, 0, tmp, 0, m_num);
			// memcpy( tmp, m_data, sizeof(_T)*m_num );
			m_size = size;
			// delete[] m_data;
			m_data = null;
			m_data = tmp;
		}

		void clean() {
			m_num = 0;
		}

		void sorted_insert(Integer x) {
			if (m_num == 0) {
				push_back(x);
				return;
			}

			if (m_num == m_size)
				re_allocate(m_size * 2);

			int l, r;

			for (l = 0, r = m_num; l < r;) {
				int m = (l + r) / 2;
				if (m_data[m].compareTo(x) == 1)
					l = m + 1;
				else
					r = m;
			}

			if (l < m_num && m_data[l].compareTo(x) == 0) {
				// printf("Insert Duplicate....\n");
				// cout<<x<<endl;
				// break;
			} else {
				if (m_num > l) {
					int k = m_num;
					while (k >= (m_num - l)) {
						m_data[k] = m_data[k - 1];
						k--;
					}
					// System.arraycopy(m_data,0,tmp,0,m_num);
					// memmove( m_data+l+1, m_data+l, sizeof(_T)*(m_num-l) );
				}
				m_num++;
				m_data[l] = x;
			}
		}
		// bool remove_unsorted( _T& x )
		// {
		// for ( int m = 0 ; m < m_num ; ++m )
		// {
		// if ( m_data[m] == x )
		// {
		// m_num--;
		// if ( m_num > m ) memcpy( m_data+m, m_data+m+1, sizeof(_T)*(m_num-m)
		// );
		// return true;
		// }
		// }
		// return false;
		// }

		// _T& operator[]( unsigned int i )
		// {
		// //if ( i < 0 || i >= m_num )
		// //{
		// // printf("iVector [] out of range!!!\n");
		// //}
		// return m_data[i];
		// }

		Integer get(int i) {
			// if ( i < 0 || i >= m_num )
			// {
			// printf("iVector [] out of range!!!\n");
			// }
			return m_data[i];
		}
	}

	public class VectorLabelQunituple {
		public LabelQunituple m_data[] = new LabelQunituple[VectorDefaultSize];
		int m_size = VectorDefaultSize;

		int m_num = 0;

		VectorLabelQunituple() {
			// printf("%d\n",VectorDefaultSize);
			m_size = VectorDefaultSize;
			for (int i = 0; i < VectorDefaultSize; i++)
				m_data[i] = new LabelQunituple();
			m_num = 0;
		}

		VectorLabelQunituple(int n) {
			if (n == 0) {
				n = VectorDefaultSize;
			}
			// printf("iVector allocate: %d\n",n);
			m_size = n;
			for (int i = 0; i < n; i++)
				m_data[i] = new LabelQunituple();
			m_num = 0;
		}

		void push_back(LabelQunituple d) {
			if (m_num == m_size) {
				re_allocate(m_size * 2);
			}
			m_data[m_num] = d;
			m_num++;
		}
		// void push_back( const _T* p, unsigned int len )
		// {
		// while ( m_num + len > m_size )
		// {
		// re_allocate( m_size*2 );
		// }
		// memcpy( m_data+m_num, p, sizeof(_T)*len );
		// m_num += len;
		// }

		void re_allocate(int size) {
			if (size < m_num) {
				return;
			}
			LabelQunituple tmp[] = new LabelQunituple[size];
			for(int i=0;i<size;i++){
				tmp[i]=new LabelQunituple();
			}
			System.arraycopy(m_data, 0, tmp, 0, m_num);
			// memcpy( tmp, m_data, sizeof(_T)*m_num );
			m_size = size;
			// delete[] m_data;
			m_data = null;
			m_data = tmp;
		}

		void clean() {
			m_num = 0;
		}

		void sorted_insert(LabelQunituple x) {
			if (m_num == 0) {
				push_back(x);
				return;
			}

			if (m_num == m_size)
				re_allocate(m_size * 2);

			int l, r;

			for (l = 0, r = m_num; l < r;) {
				int m = (l + r) / 2;
				if (m_data[m].compareTo(x) == 1)
					l = m + 1;
				else
					r = m;
			}

			if (l < m_num && m_data[l].compareTo(x) == 0) {
				// printf("Insert Duplicate....\n");
				// cout<<x<<endl;
				// break;
			} else {
				if (m_num > l) {
					int k = m_num;
					while (k >= (l + 1)) {
						m_data[k] = m_data[k - 1];
						k--;
					}
					// System.arraycopy(m_data,0,tmp,0,m_num);
					// memmove( m_data+l+1, m_data+l, sizeof(_T)*(m_num-l) );
				}
				m_num++;
				m_data[l] = x;
			}
		}

		// bool remove_unsorted( _T& x )
		// {
		// for ( int m = 0 ; m < m_num ; ++m )
		// {
		// if ( m_data[m] == x )
		// {
		// m_num--;
		// if ( m_num > m ) memcpy( m_data+m, m_data+m+1, sizeof(_T)*(m_num-m)
		// );
		// return true;
		// }
		// }
		// return false;
		// }

		// _T& operator[]( unsigned int i )
		// {
		// //if ( i < 0 || i >= m_num )
		// //{
		// // printf("iVector [] out of range!!!\n");
		// //}
		// return m_data[i];
		// }

		LabelQunituple get(int i) {
			// if ( i < 0 || i >= m_num )
			// {
			// printf("iVector [] out of range!!!\n");
			// }
			return m_data[i];
		}
	}

	public class VectorKeyValue {
		public Key_Value m_data[] = new Key_Value[VectorDefaultSize];
		int m_size = VectorDefaultSize;

		int m_num = 0;

		VectorKeyValue() {
			// printf("%d\n",VectorDefaultSize);
			m_size = VectorDefaultSize;
			for (int i = 0; i < VectorDefaultSize; i++)
				m_data[i] = new Key_Value();
			m_num = 0;
		}

		VectorKeyValue(int n) {
			if (n == 0) {
				n = VectorDefaultSize;
			}
			// printf("iVector allocate: %d\n",n);
			m_size = n;
			for (int i = 0; i < n; i++)
				m_data[i] = new Key_Value();
			m_num = 0;
		}

		void push_back(Key_Value d) {
			if (m_num == m_size) {
				re_allocate(m_size * 2);
			}
			m_data[m_num] = d;
			m_num++;
		}
		// void push_back( const _T* p, unsigned int len )
		// {
		// while ( m_num + len > m_size )
		// {
		// re_allocate( m_size*2 );
		// }
		// memcpy( m_data+m_num, p, sizeof(_T)*len );
		// m_num += len;
		// }

		void re_allocate(int size) {
			if (size < m_num) {
				return;
			}
			Key_Value tmp[] = new Key_Value[size];
			for(int i=0;i<size;i++)
				tmp[i]=new Key_Value();
			System.arraycopy(m_data, 0, tmp, 0, m_num);
			// memcpy( tmp, m_data, sizeof(_T)*m_num );
			m_size = size;
			// delete[] m_data;
			m_data = null;
			m_data = tmp;
		}

		void clean() {
			m_num = 0;
		}

		void sorted_insert(Key_Value x) {
			if (m_num == 0) {
				push_back(x);
				return;
			}

			if (m_num == m_size)
				re_allocate(m_size * 2);

			int l, r;

			for (l = 0, r = m_num; l < r;) {
				int m = (l + r) / 2;
				if (m_data[m].compareTo(x) == 1)
					l = m + 1;
				else
					r = m;
			}

			if (l < m_num && m_data[l].compareTo(x) == 0) {
				// printf("Insert Duplicate....\n");
				// cout<<x<<endl;
				// break;
			} else {
				if (m_num > l) {
					int k = m_num;
					while (k >= (m_num - l)) {
						m_data[k] = m_data[k - 1];
						k--;
					}
					// System.arraycopy(m_data,0,tmp,0,m_num);
					// memmove( m_data+l+1, m_data+l, sizeof(_T)*(m_num-l) );
				}
				m_num++;
				m_data[l] = x;
			}
		}

		// bool remove_unsorted( _T& x )
		// {
		// for ( int m = 0 ; m < m_num ; ++m )
		// {
		// if ( m_data[m] == x )
		// {
		// m_num--;
		// if ( m_num > m ) memcpy( m_data+m, m_data+m+1, sizeof(_T)*(m_num-m)
		// );
		// return true;
		// }
		// }
		// return false;
		// }

		// _T& operator[]( unsigned int i )
		// {
		// //if ( i < 0 || i >= m_num )
		// //{
		// // printf("iVector [] out of range!!!\n");
		// //}
		// return m_data[i];
		// }

		Key_Value get(int i) {
			// if ( i < 0 || i >= m_num )
			// {
			// printf("iVector [] out of range!!!\n");
			// }
			return m_data[i];
		}
	}

	public class Vector2 {
		public VectorEdgeQuadruple m_data[] = new VectorEdgeQuadruple[VectorDefaultSize];
		public int m_size;

		public int m_num;

		public Vector2() {
			// printf("%d\n",VectorDefaultSize);
			m_size = VectorDefaultSize;
			for (int i = 0; i < m_size; i++) {
				m_data[i] = new VectorEdgeQuadruple();
			}

			m_num = 0;
		}

		public Vector2(int n) {
			if (n == 0) {
				n = VectorDefaultSize;
			}
			// printf("iVector allocate: %d\n",n);
			m_size = n;
			for (int i = 0; i < n; i++) {
				m_data[i] = new VectorEdgeQuadruple();
			}
			m_num = 0;
		}

		void push_back(VectorEdgeQuadruple d) {
			if (m_num == m_size) {
				re_allocate(m_size * 2);
			}
			m_data[m_num] = d;
			m_num++;
		}
		// void push_back( const _T* p, unsigned int len )
		// {
		// while ( m_num + len > m_size )
		// {
		// re_allocate( m_size*2 );
		// }
		// memcpy( m_data+m_num, p, sizeof(_T)*len );
		// m_num += len;
		// }

		void re_allocate(int size) {
			if (size < m_num) {
				return;
			}
			VectorEdgeQuadruple tmp[] = new VectorEdgeQuadruple[size];
			for (int i = 0; i < size; i++)
				tmp[i] = new VectorEdgeQuadruple();
			System.arraycopy(m_data, 0, tmp, 0, m_num);
			// memcpy( tmp, m_data, sizeof(_T)*m_num );
			m_size = size;
			// delete[] m_data;
			m_data = null;
			m_data = tmp;
		}

		void clean() {
			m_num = 0;
		}

		// void sorted_insert( VectorEdgeQuadruple x )
		// {
		// if ( m_num == 0 )
		// {
		// push_back( x );
		// return;
		// }
		//
		// if ( m_num == m_size ) re_allocate( m_size*2 );
		//
		// int l,r;
		//
		// for ( l = 0 , r = m_num ; l < r ; )
		// {
		// int m = (l+r)/2;
		// if ( m_data[m].compareTo(x)==1) l = m+1;
		// else r = m;
		// }
		//
		// if ( l < m_num && m_data[l].compareTo(x)==0 )
		// {
		// //printf("Insert Duplicate....\n");
		// //cout<<x<<endl;
		// // break;
		// }
		// else
		// {
		// if ( m_num > l )
		// {
		// int k=m_num;
		// while(k>=(m_num-l)){
		// m_data[k]=m_data[k-1];
		// k--;
		// }
		// //System.arraycopy(m_data,0,tmp,0,m_num);
		// //memmove( m_data+l+1, m_data+l, sizeof(_T)*(m_num-l) );
		// }
		// m_num++;
		// m_data[l] = x;
		// }
		// }

		// bool remove_unsorted( _T& x )
		// {
		// for ( int m = 0 ; m < m_num ; ++m )
		// {
		// if ( m_data[m] == x )
		// {
		// m_num--;
		// if ( m_num > m ) memcpy( m_data+m, m_data+m+1, sizeof(_T)*(m_num-m)
		// );
		// return true;
		// }
		// }
		// return false;
		// }

		// _T& operator[]( unsigned int i )
		// {
		// //if ( i < 0 || i >= m_num )
		// //{
		// // printf("iVector [] out of range!!!\n");
		// //}
		// return m_data[i];
		// }

		VectorEdgeQuadruple get(int i) {
			// if ( i < 0 || i >= m_num )
			// {
			// printf("iVector [] out of range!!!\n");
			// }
			return m_data[i];
		}
	}

	public class Vector22 {
		public VectorLabelQunituple m_data[] = new VectorLabelQunituple[VectorDefaultSize];
		public int m_size;

		public int m_num;

		public Vector22() {
			// printf("%d\n",VectorDefaultSize);
			m_size = VectorDefaultSize;
			for (int i = 0; i < m_size; i++) {
				m_data[i] = new VectorLabelQunituple();
			}

			m_num = 0;
		}

		public Vector22(int n) {
			if (n == 0) {
				n = VectorDefaultSize;
			}
			// printf("iVector allocate: %d\n",n);
			m_size = n;
			for (int i = 0; i < n; i++) {
				m_data[i] = new VectorLabelQunituple();
			}
			m_num = 0;
		}

		void push_back(VectorLabelQunituple d) {
			if (m_num == m_size) {
				re_allocate(m_size * 2);
			}
			m_data[m_num] = d;
			m_num++;
		}
		// void push_back( const _T* p, unsigned int len )
		// {
		// while ( m_num + len > m_size )
		// {
		// re_allocate( m_size*2 );
		// }
		// memcpy( m_data+m_num, p, sizeof(_T)*len );
		// m_num += len;
		// }

		void re_allocate(int size) {
			if (size < m_num) {
				return;
			}
			VectorLabelQunituple tmp[] = new VectorLabelQunituple[size];
			for (int i = 0; i < size; i++)
				tmp[i] = new VectorLabelQunituple();
			System.arraycopy(m_data, 0, tmp, 0, m_num);
			// memcpy( tmp, m_data, sizeof(_T)*m_num );
			m_size = size;
			// delete[] m_data;
			m_data = null;
			m_data = tmp;
		}

		void clean() {
			m_num = 0;
		}

		// void sorted_insert( VectorEdgeQuadruple x )
		// {
		// if ( m_num == 0 )
		// {
		// push_back( x );
		// return;
		// }
		//
		// if ( m_num == m_size ) re_allocate( m_size*2 );
		//
		// int l,r;
		//
		// for ( l = 0 , r = m_num ; l < r ; )
		// {
		// int m = (l+r)/2;
		// if ( m_data[m].compareTo(x)==1) l = m+1;
		// else r = m;
		// }
		//
		// if ( l < m_num && m_data[l].compareTo(x)==0 )
		// {
		// //printf("Insert Duplicate....\n");
		// //cout<<x<<endl;
		// // break;
		// }
		// else
		// {
		// if ( m_num > l )
		// {
		// int k=m_num;
		// while(k>=(m_num-l)){
		// m_data[k]=m_data[k-1];
		// k--;
		// }
		// //System.arraycopy(m_data,0,tmp,0,m_num);
		// //memmove( m_data+l+1, m_data+l, sizeof(_T)*(m_num-l) );
		// }
		// m_num++;
		// m_data[l] = x;
		// }
		// }

		// bool remove_unsorted( _T& x )
		// {
		// for ( int m = 0 ; m < m_num ; ++m )
		// {
		// if ( m_data[m] == x )
		// {
		// m_num--;
		// if ( m_num > m ) memcpy( m_data+m, m_data+m+1, sizeof(_T)*(m_num-m)
		// );
		// return true;
		// }
		// }
		// return false;
		// }

		// _T& operator[]( unsigned int i )
		// {
		// //if ( i < 0 || i >= m_num )
		// //{
		// // printf("iVector [] out of range!!!\n");
		// //}
		// return m_data[i];
		// }

		VectorLabelQunituple get(int i) {
			// if ( i < 0 || i >= m_num )
			// {
			// printf("iVector [] out of range!!!\n");
			// }
			return m_data[i];
		}
	}

	

	public class Vector3 {
		public Vector22 m_data[] = new Vector22[VectorDefaultSize];
		public int m_size;

		public int m_num;

		public Vector3() {
			// printf("%d\n",VectorDefaultSize);
			m_size = VectorDefaultSize;
			for (int i = 0; i < m_size; i++)
				m_data[i] = new Vector22();
			m_num = 0;
		}

		public Vector3(int n) {
			if (n == 0) {
				n = VectorDefaultSize;
			}
			// printf("iVector allocate: %d\n",n);
			m_size = n;
			for (int i = 0; i < n; i++)
				m_data[i] = new Vector22();
			m_num = 0;
		}

		void push_back(Vector22 d) {
			if (m_num == m_size) {
				re_allocate(m_size * 2);
			}
			m_data[m_num] = d;
			m_num++;
		}
		// void push_back( const _T* p, unsigned int len )
		// {
		// while ( m_num + len > m_size )
		// {
		// re_allocate( m_size*2 );
		// }
		// memcpy( m_data+m_num, p, sizeof(_T)*len );
		// m_num += len;
		// }

		void re_allocate(int size) {
			if (size < m_num) {
				return;
			}
			Vector22 tmp[] = new Vector22[size];
			for(int i=0;i<size;i++)
				tmp[i]=new Vector22();
			System.arraycopy(m_data, 0, tmp, 0, m_num);
			// memcpy( tmp, m_data, sizeof(_T)*m_num );
			m_size = size;
			// delete[] m_data;
			m_data = null;
			m_data = tmp;
		}

		void clean() {
			m_num = 0;
		}

		// void sorted_insert( VectorEdgeQuadruple x )
		// {
		// if ( m_num == 0 )
		// {
		// push_back( x );
		// return;
		// }
		//
		// if ( m_num == m_size ) re_allocate( m_size*2 );
		//
		// int l,r;
		//
		// for ( l = 0 , r = m_num ; l < r ; )
		// {
		// int m = (l+r)/2;
		// if ( m_data[m].compareTo(x)==1) l = m+1;
		// else r = m;
		// }
		//
		// if ( l < m_num && m_data[l].compareTo(x)==0 )
		// {
		// //printf("Insert Duplicate....\n");
		// //cout<<x<<endl;
		// // break;
		// }
		// else
		// {
		// if ( m_num > l )
		// {
		// int k=m_num;
		// while(k>=(m_num-l)){
		// m_data[k]=m_data[k-1];
		// k--;
		// }
		// //System.arraycopy(m_data,0,tmp,0,m_num);
		// //memmove( m_data+l+1, m_data+l, sizeof(_T)*(m_num-l) );
		// }
		// m_num++;
		// m_data[l] = x;
		// }
		// }

		// bool remove_unsorted( _T& x )
		// {
		// for ( int m = 0 ; m < m_num ; ++m )
		// {
		// if ( m_data[m] == x )
		// {
		// m_num--;
		// if ( m_num > m ) memcpy( m_data+m, m_data+m+1, sizeof(_T)*(m_num-m)
		// );
		// return true;
		// }
		// }
		// return false;
		// }

		// _T& operator[]( unsigned int i )
		// {
		// //if ( i < 0 || i >= m_num )
		// //{
		// // printf("iVector [] out of range!!!\n");
		// //}
		// return m_data[i];
		// }

		Vector22 get(int i) {
			// if ( i < 0 || i >= m_num )
			// {
			// printf("iVector [] out of range!!!\n");
			// }
			return m_data[i];
		}
	}

	public  class HHL_temporal {
		// ttr tt[]=new ttr[2];
		public int N, M, init;
		long lcnt, new_cnt;
		long tmin, tmax;// the smallest timestamp and largest timestamp in the
						// graph
		public int vertexid2level[];// obtain the level order from the vertex id;
		public int leveltovertexid[];// obtain the vertex id from the level order;
		int in_deg[], out_deg[], p[], tb[], q[], fq[], tq[];
		boolean compressed_or_not;
		double ordering_time;
		double indexing_time;
		int stamps;
		Vector<Vector<tri>> fa,ch;
		// vector<vector<pair<int,int>>>cv;
		Vector<Integer>chv;
		Vector2 links0=new Vector2();
		Vector2 links1=new Vector2();
		//iVector<iVector<EdgeQuadruple>> links0 = new iVector<iVector<EdgeQuadruple>>();
		//iVector<iVector<EdgeQuadruple>> links1 = new iVector<iVector<EdgeQuadruple>>();
		
		//iVector<iVector<Integer>> static_edges0 = new iVector<iVector<Integer>>();
		//iVector<iVector<Integer>> static_edges1 = new iVector<iVector<Integer>>();
		
		Vector3 label0=new Vector3();
		Vector3 label1=new Vector3();
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
//				static_edges0.re_allocate(N);
//				static_edges0.m_num = N;
//				static_edges1.re_allocate(N);
//				static_edges1.m_num = N;
				// pq[0].initialize(N);
				// pq[1].initialize(N);
				// opq.initialize(N);
				mark.initialize(N);
				busmark.initialize(N);
				fathermark.initialize(N);
				vertexid2level = new int[N];
				leveltovertexid = new int[N];
				in_deg = new int[N];
				out_deg = new int[N];
				// memset(in_deg, 0x00, 4*N);
				// memset(out_deg, 0x00, sizeof(int)*N);
				tmin = MAX_TIMESTAMP;
				tmax = -1;
				int x = 0, y = 0, w = 0, lines = 0;
				long t = 0;
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
						out_deg[x]++;
						in_deg[y]++;
					}
				}
				
//				BufferedWriter bw = new BufferedWriter(new FileWriter("D:\\gz\\v2e.txt"));
//				for(int i=0; i<N; i++)
//				{
//					for(int j=links0.get(i).m_num-1;j>=0;j--)
//					{
//						bw.write(Integer.toString(i));
//						bw.write(" ");
//						bw.write(Integer.toString(j));///���źͱ���Ķ�Ӧ��ϵ������level�ͱ���Ķ�Ӧ��ϵ����ʹ��level����Ҫ��
//						bw.write("\n");
//					}
//				}
//				bw.flush();
//				bw.close();
//				BufferedWriter bw = new BufferedWriter(new FileWriter("D:\\gz\\austinGraphJava.txt"));
//				bw.write(Integer.toString(N));
//				bw.write("\n");
//				for(int i=0; i<N; i++){
//					//fprintf(index_file, "%d:", i);
//					bw.write(Integer.toString(i));
//					bw.write(":");
//					for(int j=0; j<links0.get(i).m_num; j++)
//					{
//							String v=Integer.toString(links0.get(i).get(j).v);
//							String ta=Integer.toString((int) links0.get(i).get(j).ta);
//							String tb=Integer.toString((int) (links0.get(i).get(j).tb));
//							String Busid=Integer.toString(links0.get(i).get(j).busid);
//							String s=v+" "+ta+" "+tb+" "+Busid+" ";
//							bw.write(s);
//					}
//					bw.write("-1 ");
//					for(int j=0; j<links1.get(i).m_num; j++)
//						
//						{
//						String v=Integer.toString(links1.get(i).get(j).v);
//						String ta=Integer.toString((int) links1.get(i).get(j).ta);
//						String tb=Integer.toString((int) (links1.get(i).get(j).tb));
//						String Busid=Integer.toString(links1.get(i).get(j).busid);
//						String s=v+" "+ta+" "+tb+" "+Busid+" ";
//						bw.write(s);
//						}
//					bw.write( "-2\n");
//				}
//				bw.flush();
//				bw.close();
//				System.out.println("output graph complete");
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
		void load_order_file(String file_name){
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
		
		
		void func(){
	    	for(int i=0; i<N; i++){
	    	            leveltovertexid[vertexid2level[i]] = i;
	    	        }
	    }
		
		 void add_Label(Vector22  insert_list, LabelQunituple new_label){
		        int last_list_index = insert_list.m_num -1;
		        if(last_list_index>=0 && insert_list.get(last_list_index).get(0).v == new_label.v){
		            insert_list.get(last_list_index).sorted_insert(new_label);
		        }else{
		            VectorLabelQunituple new_list=new VectorLabelQunituple();
		            new_list.sorted_insert(new_label);
		            insert_list.push_back(new_list);
		        }
		    }
		 
		 int Search_Label_Qunituple(long timestamp, VectorLabelQunituple label_list){
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
		 
		    boolean reachable_query(int src, int tar, long timerange_a, long timerange_b){
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
		
		    int Search_Edge_Quadruple(long timestamp, VectorEdgeQuadruple edge_list, int search_case){
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
		            for(int k=links0.get(v).m_num-1; k>=0; k--){//
		            	for(ii=0;ii<N;ii++)
		            		w_[ii]=-1;
						if(leveltovertexid[links0.get(v).get(k).v]<=i)continue;//links[v][k]�е�k�Ƕ���v�ĵ�k�����ߣ���������ָ��Ķ���id��links[v][k].v
						int start_time = (int) links0.get(v).get(k).ta;
		                int startBus=links0.get(v).get(k).busid;///��ѯƥ��ʱlabel_in��Ҫ֪��·���ϵ�һ̨������
		                mark.insert(v, (int)start_time);
		              
		               /// for( ; k>=0 && start_time == links0.get(v).get(k).ta ; k--){///����ʱ����ͬ�����Ų�ͬ������ȥ�����Գ���ʱ����ͬ�ı�
		                    int same_timestamp_vertex = links0.get(v).get(k).v;
							if(leveltovertexid[same_timestamp_vertex]<=i)continue;
		                    long ts = links0.get(v).get(k).tb;
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
		                        query_range_end = MAX_TIMESTAMP;
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
		                            w_end_time = MAX_TIMESTAMP;
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
		                            		                          
		                            
//		                            if(busmark.m_data[father] == busid)
//		                            {
//		                            	busmark.insert(w, busid);
//		                            	walk_time=0;
//		                            }
//		                            else 
//		                            {
//		                            	busmark.insert(w, -1);
//		                            	walk_time=3;
//		                            }
		                          //  mark.insert(w,  (int) tbb+walk_time);
		                           // nodes_pq[0].insert(w, tbb - start_time+walk_time);
		                            ///
		                            
		                            fathermark.insert(w, father);
//									if(tbb-start_time==w_end_time-w_start_time)printf("%d",w_[w]);
									w_[w]=w_[father];
									if(w_[w]<0||leveltovertexid[father]<w_[w])
										w_[w]=leveltovertexid[father];
//									if(tbb-start_time==w_end_time-w_start_time)printf(" %d\n",w_[w]);
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
		                        	int tb = (int) mark.m_data[j];
		                            int father = (int) fathermark.m_data[j];
		                            int busid = (int) busmark.m_data[j];
		                            assert(father>=0);
		                            LabelQunituple new_p=new LabelQunituple(leveltovertexid[v],ta, tb, leveltovertexid[father], w_[j], busid,startBus); 
		                            //label[1][leveltovertexid[j]].sorted_insert(new_p);
		                            add_Label(label1.get(leveltovertexid[j]), new_p); 
//									printf("%d in: %d %d %d %d %d\n",j,v,ta,tb,father,busid);
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
		            	int end_time =  (int) links1.get(v).get(k).tb;
		                int endBus=links1.get(v).get(k).busid;///��ѯƥ��ʱlabel_out��Ҫ֪��·���ϵ��������
		                mark.insert(v, (int) end_time );
		               /// for( ; k< links1.get(v).m_num && end_time == links1.get(v).get(k).tb ; k++){
		                    int same_timestamp_vertex = links1.get(v).get(k).v;
		                    if(leveltovertexid[same_timestamp_vertex]<=i)continue;
		                    int ts = (int) links1.get(v).get(k).ta;

		                    mark.insert(same_timestamp_vertex, (int) ts );
		                    busmark.insert(same_timestamp_vertex, links1.get(v).get(k).busid);
		                    fathermark.insert(same_timestamp_vertex, v);
		                    nodes_pq[1].insert(same_timestamp_vertex, end_time -ts,links1.get(v).get(k).busid);
							w_[same_timestamp_vertex]=-2;
		              ///  }
		               /// k--;
		                LabelQunituple p=new LabelQunituple(leveltovertexid[v], (int)links1.get(v).get(k).tb, (int)links1.get(v).get(k).tb, -1, -2, -1,endBus);
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
//									if(end_time-taa==w_end_time-w_start_time)printf("%d",w_[w]);
									w_[w]=w_[father];
									if(w_[w]<0||leveltovertexid[father]<w_[w])w_[w]=leveltovertexid[father];
//									if(end_time-taa==w_end_time-w_start_time)printf(" %d\n",w_[w]);
		                        }
		                    }
		                }
		                for( int counter = 0; counter < mark.occur.m_num; counter++){
		                    if(mark.exist(mark.occur.get(counter))){
		                        int j = mark.occur.get(counter);
		                        if(j!=v){
		                        	int ta = (int) mark.m_data[j];
		                        	int tb = end_time;
		                            
		                            int father = (int) fathermark.m_data[j];
		                            int busid = (int) busmark.m_data[j];
		                            assert(father>=0);
		                            LabelQunituple new_p=new LabelQunituple(leveltovertexid[v],ta, tb, leveltovertexid[father], w_[j], busid,endBus);
		                            add_Label(label0.get(leveltovertexid[j]), new_p);
//									printf("%d out: %d %d %d %d %d\n",j,v,ta,tb,father,busid);
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
		
		public HHL_temporal(){
			
		}
		
		LabelQunituple get_address(int v1,int v2,long ta,long tb,Vector3 lout,Vector3 lin)
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
		
		void children_pointer_linking(){
	        for(int i=0; i<N; i++){
	            for(int j=0; j<label0.get(i).m_num; j++){
	                if(label0.get(i).get(j).get(0).v==i)continue;
	                for( int k=0; k<label0.get(i).get(j).m_num; k++){
	                    int w=label0.get(i).get(j).get(k).w;
	                    if(w>=0)
	                    {
//	                    	if(i==9&&j==7)
//	                    	{
//	                    		System.out.println("debug");
//	                    	}
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
//	                    	if(i==9&&j==7)
//	                    	{
//	                    		System.out.println("debug");
//	                    	}
	                    	label1.get(i).get(j).get(k).iw=get_address(label1.get(i).get(j).get(k).v,w,label1.get(i).get(j).get(k).ta,-1,label0,label1);
	                        label1.get(i).get(j).get(k).wv=get_address(w,i,-1,label1.get(i).get(j).get(k).tb,label0,label1);                  
	                        ///label1.get(i).get(j).get(k).iw=get_address(w,i,-1,label1.get(i).get(j).get(k).tb,label0,label1);
	                        ///label1.get(i).get(j).get(k).wv=get_address(label1.get(i).get(j).get(k).v,w,label1.get(i).get(j).get(k).ta,-1,label0,label1);
	                    
	                    }
	                }
	            }
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
//							if(inlabel_tb>earliest_arrival.second)
//								{k++;}
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
	      ///��·��ʱ�������ĳ��˲�չ��bus��ͬ��ÿһ����
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
//	        for(int i=0; i<N; i++){
//	            fprintf(order_file, "%d\n", vertexid2level[i]);
//	        }
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
//		void compute_index_order_coverage_heuristic(String fn){
//	        int ql,i,j,k,m,u,v,w,tu,tv,fu,s;
//			char starting[]=new char[N];
//			
//	        tri fi;
//			//memset(starting,0,N);
//	        Random rd=new Random();
//		
//				//printf("file \"%s\" not found, all nodes will be sampled.\n",fn.c_str());
//			for(i=0;i<N;i++)
//				starting[i]=1;
//	        //puts("constructing trees...");
//	        m=0;
//	        for(i=0;i<links0.m_num;i++)
//	            m+=links0.get(i).m_num;
//	        s=m*4;
//	        fa=new Vector<Vector<tri>>(N);
//	        ch=new Vector<Vector<tri>>(N);
//	        //fa.resize(N);
//	        //ch.resize(N);
//	        p=new int[N+1];
//	        for(i=0;i<N;i++)
//	        {
//	            p[i]=i;
//	            fa.get(i).clear();
//	            ch.get(i).clear();
//	        }
//	        for(i=N-1;i!=0;i--)
//	        {
//	            j= rd.nextInt()%(i+1);//(unsigned int((rand()<<15)+rand()))%(i+1);
//	            k=p[i];
//	            p[i]=p[j];
//	            p[j]=k;
//	        }
//	        tb=new int[N];
//	        q=new int[m];
//	        fq=new int[m];
//	        tq=new int[m];
//	        for(i=0;i<N;i++)
//	        	tb[i]=-1;
//	       // memset(tb,-1,sizeof(int)*N);
//			m=0;
//	        for(i=0;i<N;i++)
//	        {
//	            if(s<1)break;
//	            v=p[i];
//				if(starting[v]==0)continue;
//	            //printf("%d nodes sampled\t\t%d space remainning...%c",m++,s,13);
//	            Vector<Integer>tbs;
//	            tbs.clear();
//	            tbs.add(v);
//				Map<Integer,Integer>tta=new HashMap<Integer,Integer>();
//				tta.clear();
//				int r=stamps;
//				for(j=links0.get(v).m_num-1;j>=0;j--)
//					if(rd.nextInt()%(j+1)<r)
//					{
//						tta.put((int) links0.get(v).get(j).ta, 1);
//						//tta[links[0][v][j].ta]=1;
//						r--;
//					}
//	            for(j=links0.get(v).m_num-1;j>=0;)
//	            {
//	                tv=(int) links0.get(v).get(j).ta;
//	                
//					if(stamps>0&&tta.containsKey(tv))
//					{
//						j--;
//						continue;
//					}
//					//printf("sampled %d %d %c",i,tv,13);
//	                tb[v]=tv;
//	                fi.v=v;
//	                fi.t=tv;
//	                fi.w=v;
//	                fa.get(v).add(fi);
//	                ql=0;
//	                while(j>=0&&links0.get(v).get(j).ta==tv)
//	                {
//	                    add_q(ql,links0.get(v).get(j).v,v,(int)links0.get(v).get(j).tb);
//	                    ql++;
//	                    j--;
//	                }
//	                while(ql!=0)
//	                {
//	                    u=q[0];
//	                    fu=fq[0];
//	                    tu=tq[0];
//	                    del_q(ql);
//	                    ql--;
//	                    if(tb[u]>=0&&tu>=tb[u])continue;
//	                    if(tb[u]<0)tbs.add(u);
//	                    tb[u]=tu;
//	                    fi.w=fu;
//	                    fa.get(u).add(fi);
//	                    fi.w=u;
//	                    ch.get(fu).add(fi);
//	                    s--;
//	                    for(k=0;k<links0.get(u).m_num;k++)
//	                    {
//	                        w=links0.get(u).get(k).v;
//	                        if(links0.get(u).get(k).ta>=tu&&(tb[w]<0||links0.get(u).get(k).tb<tb[w]))add_q(ql,w,u,(int)links0.get(u).get(k).tb);
//	                    }
//	                }
//	            }
//	            for(j=0;j<tbs.size();j++)
//	                tb[tbs.get(j)]=-1;
//	        }
//	        p[i]=-1;
//	        //puts("hashing the tree edges...");
//	        chv.clear();
//	        int as=0;
//	        for(i=0;i<N;i++)
//	        {
//	            //printf("%d %c",i,13);
//	            Vector<tri>temp=fa.get(i);
//	            q[i]=temp.size();
//	            for(fq[i]=1;fq[i]*2/3<q[i];fq[i]=fq[i]*2+1);
//	            fa.get(i).resize(fq[i]);
//	            for(j=0;j<fq[i];j++)
//	                fa[i][j].w=-1;
//	            for(j=0;j<temp.size();j++)
//	            {
//	                for(k=hash_fun(temp[j].v,temp[j].t)%fq[i];fa[i][k].w>=0;k=((k>fq[i]-2)?0:k+1),as++);
//	                fa[i][k]=temp[j];
//	            }
//	            temp=ch[i];
//	            ch[i].clear();
//	            v=-1;
//	            for(j=0;j<temp.size();j++)
//	            {
//	                if(temp[j].v!=v||temp[j].t!=tv)
//	                {
//	                    chv.push_back(-1);
//	                    ch[i].push_back(temp[j]);
//	                    ch[i][ch[i].size()-1].w=chv.size();
//	                    v=temp[j].v;
//	                    tv=temp[j].t;
//	                }
//	                chv.push_back(temp[j].w);
//	            }
//	            temp=ch[i];
//	            for(tq[i]=1;tq[i]*2/3<temp.size();tq[i]=tq[i]*2+1);
//	            ch[i].resize(tq[i]);
//	            for(j=0;j<tq[i];j++)
//	                ch[i][j].w=-1;
//	            for(j=0;j<temp.size();j++)
//	            {
//	                for(k=hash_fun(temp[j].v,temp[j].t)%tq[i];ch[i][k].w>=0;k=((k>tq[i]-2)?0:k+1),as++);
//	                ch[i][k]=temp[j];
//	            }
//	        }
//	        chv.push_back(-1);
//	        //puts("calculating coverage...");
//	        cv.resize(N);
//	        for(i=0;i<N;i++)
//	            cv[i].clear();
//	        for(i=0;p[i]>=0;i++)
//	        {
//	            //printf("%d %c",i,13);
//	            v=p[i];
//				if(!starting[v])continue;
//	            for(j=links[0][v].m_num-1;j>=0;)
//	            {
//	                for(tv=links[0][v][j].ta;j>=0&&links[0][v][j].ta==tv;j--);
//	                add_cvt(v,v,tv);
//	            }
//	        }
//	        //puts("ordering...");
//	        memset(p,0,sizeof(int)*N);
//	        for(i=0;i<N;i++)
//	        {
//	            //printf("%d %c",i,13);
//	            double max=-1;
//	            for(j=0;j<N;j++)
//	                if(!p[j])
//	                {
//	                    double cvs=(in_deg[j]+1)*(out_deg[j]+1)/1e9;
//	                    for(k=0;k<cv[j].size();k++)
//	                        cvs+=cv[j][k].second;
//	                    if(cvs>max)
//	                    {
//	                        max=cvs;
//	                        u=j;
//	                    }
//	                }
//	            vertexid2level[i]=u;
//	            p[u]=1;
//	            while(fq[u]>1)
//	            {
//	                while(1)
//	                {
//	                    j = rand()%fq[u];
//	                    //j=(unsigned int((rand()<<15)+rand()))%fq[u];
//	                    v=fa[u][j].v;
//	                    tv=fa[u][j].t;
//	                    w=fa[u][j].w;
//	                    if(w>=0)break;
//	                }
//	                j=del_tr(u,v,tv);
//	                if(w==u)continue;
//	                k=u;
//	                while(w!=k)
//	                {
//	                    add_cv(w,v,-j);
//	                    k=w;
//	                    w=get_fa(k,v,tv);
//	                }
//	            }
//	        }
//	        end_time = clock();
//	        ordering_time = (double)(end_time-start_time)/CLOCKS_PER_SEC ;
//	        cout<<"Ordering time: "<<ordering_time<<endl;
//	        //printf( "Ordering time: %0.2lf\n",(double)(end_time-start_time)/CLOCKS_PER_SEC );
//	        delete []in_deg;
//	        delete []out_deg;
//	        delete []p;
//	        delete []tb;
//	        delete []q;
//	        delete []fq;
//	        delete []tq;
//	    }
//		
//		void add_q(int ql,int v,int f,int t)
//	    {
//	        int i,j,k;
//	        q[ql]=v;
//	        fq[ql]=f;
//	        tq[ql]=t;
//	        i=ql++;
//	        while(i!=0)
//	        {
//	            j=(i-1)/2;
//	            if(tq[i]<tq[j])
//	            {
//	                k=q[i];
//	                q[i]=q[j];
//	                q[j]=k;
//	                k=fq[i];
//	                fq[i]=fq[j];
//	                fq[j]=k;
//	                k=tq[i];
//	                tq[i]=tq[j];
//	                tq[j]=k;
//	            }
//	            else break;
//	            i=j;
//	        }
//	    }
//
//	    void del_q(int ql)
//	    {
//	        int i,j,k;
//	        ql--;
//	        q[0]=q[ql];
//	        fq[0]=fq[ql];
//	        tq[0]=tq[ql];
//	        i=0;
//	        while(1)
//	        {
//	            j=i*2+1;
//	            if(j>=ql)break;
//	            if(j+1<ql&&tq[j+1]<tq[j])j++;
//	            if(tq[j]<tq[i])
//	            {
//	                k=q[i];
//	                q[i]=q[j];
//	                q[j]=k;
//	                k=fq[i];
//	                fq[i]=fq[j];
//	                fq[j]=k;
//	                k=tq[i];
//	                tq[i]=tq[j];
//	                tq[j]=k;
//	            }
//	            else break;
//	            i=j;
//	        }
//	    }
//
//	    int add_cvt(int u,int v,int t)
//	        {
//	            int s=1;
//	            for(int i=get_ch(u,v,t);chv[i]>=0;i++)
//	                s+=add_cvt(chv[i],v,t);
//	            add_cv(u,v,s);
//	            return s;
//	        }
//	    void add_cv(int u,int v,int s)
//	        {
//	            int i;
//	            for(i=0;i<cv[u].size();i++)
//	                if(cv[u][i].first==v)
//	                {
//	                    cv[u][i].second+=s;
//	                    return;
//	                }
//	            cv[u].push_back(make_pair(v,s));
//	        }
//
//	    int get_ch(int u,int v,int t)
//	        {
//	            int r;
//	            r=hash_fun(v,t)%tq[u];
//	            while(1)
//	            {
//	                if(ch[u][r].w<0)return 0;
//	                if(ch[u][r].v==v&&ch[u][r].t==t)return ch[u][r].w;
//	                r++;
//	                if(r==tq[u])r=0;
//	            }
//	        }
//
	}

	public static void main(String[] args) throws Exception{
		//TTL ttl=new TTL();
		
		args = new String[]{
				"hdfs://192.168.112.128:8020/user/cyhp/mr/cluster-test/input",
				"hdfs://192.168.112.128:8020/user/cyhp/mr/cluster-test/output5"
		};
		
		
		HHL_temporal2 hhl =new HHL_temporal2();
		hhl.init=1;
		int i,j;
//		hhl.load_order_file("D:\\myttl\\heuristic-austin.index.order");
//		hhl.compute_index_with_order_BFS();
	//	hhl.output("D:\\myttl\\heuristic-austinJava");
		long startTime=System.currentTimeMillis();
		//hhl.load_temporal_graph("/opt/TTL_source/gz.txt");
		long endTime=System.currentTimeMillis();
	
		double diff=(double)(endTime-startTime)/1000;
		System.out.printf("Load graph time:%.2fs\n",diff);
	
		//hhl.load_temporal_graph("D:\\myttl\\exampleJava.txt");
		//hhl.load_order_file("/opt/TTL_source/gz.order");
		//hhl.func();
		//for(i=0;i<hhl.N;i++)
			//hhl.vertexid2level[i] = i;
		//hhl.load_order_file("D:\\myttl\\example.order");
		startTime=System.currentTimeMillis();
		
		
		Configuration conf=new Configuration();
		hhl.N=64;
		conf.setInt("nNodes", hhl.N);//
		


		FileSystem fs=FileSystem.get(URI.create(""), conf);

		//SequenceFile.Writer writer=SequenceFile.createWriter(fc, conf, name, keyClass, valClass, compressionType, codec, metadata, createFlag, opts)
		
		Job job=Job.getInstance(conf,HHL_temporal2.class.getSimpleName());
		
		job.setJarByClass(HHL_temporal2.class);
		job.setMapperClass(CombineMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		//job.setMapOutputValueClass(Vector22Writable.class);
		//job.setOutputFormatClass(SequenceFileOutputFormat.class);

		//FileInputFormat.addInputPath(job, new Path("hdfs://192.168.112.128:8020/user/ningzhq/example/example_map_order"));
		//FileInputFormat.addInputPath(job, new Path("hdfs://192.168.112.128:8020/user/ningzhq/gz_map_order"));
		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.112.128:8020/user/ningzhq/austin/austin_map_order"));
		
		Date date=new Date();
		DateFormat format=new SimpleDateFormat("-yyyyMMdd-HH-mm-ss");
		String time=format.format(date);
		
		
		String pathStr="hdfs://192.168.112.128:8020/user/ningzhq/gz_output/output1"+time;
		FileOutputFormat.setOutputPath(job, new Path(pathStr));//+Integer.toString(tmp)));
		//job.setNumReduceTasks(0);    
		//hhl.compute_index_with_order_BFS();
		
		//job.waitForCompletion(true);
		
//		Configuration conf2=new Configuration();
//		conf2.setInt("nNodes", hhl.N);//
//		
//		Job job2=Job.getInstance(conf2,HHL_temporal2.class.getSimpleName());
//		
//		job2.setJarByClass(HHL_temporal2.class);
//		job2.setMapperClass(MyMapper2.class);
//		job2.setReducerClass(MyReducer2.class);
//		job2.setOutputKeyClass(IntWritable.class);
//		job2.setOutputValueClass(Text.class);
		//job2.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		//FileInputFormat.addInputPath(job2, new Path("hdfs://192.168.112.128:8020/user/ningzhq/gz_map_order"));

		FileInputFormat.setMinInputSplitSize(job, 1);
		//FileInputFormat.setMinInputSplitSize(job2, 1);
		
		//FileInputFormat.setMaxInputSplitSize(job, 5000);
		FileInputFormat.setMaxInputSplitSize(job, 5000);
		//FileInputFormat.setMaxInputSplitSize(job2, 47);
		
		//String pathStr2="hdfs://192.168.112.128:8020/user/ningzhq/gz_output/output2"+time;
		//FileOutputFormat.setOutputPath(job2, new Path(pathStr2));
		//job2.setNumReduceTasks(0);    
		//hhl.compute_index_with_order_BFS();
		
		//job2.waitForCompletion(true);
		
		//job.submit();
		//job2.submit();
		//job.isComplete();
		//job2.isComplete();
		boolean success=job.waitForCompletion(true);
		//boolean success2=job2.waitForCompletion(true);
		
		//hhl.addInitialIndex();
		//hhl.children_pointer_linking();
		//hhl.output("D:\\gz\\gz");
		endTime=System.currentTimeMillis();
		diff=(double)(endTime-startTime)/1000;
		System.out.printf("Generate labels time:%.2fs\n",diff);
	
		//hhl.load_query_set("/opt/TTL_source/query-gz.txt");

		//SequenceFile.Reader reader=new SequenceFile.Reader(fs, new Path(pathStr), conf);
         
       //  Vector3_for_map labels=new Vector3_for_map();
       //  labels.re_allocate(hhl.N);
       //  Vector22Writable value=new Vector22Writable();
        // IntWritable key=new IntWritable();
       //  while(reader.next(key, value)){
         //    int k=key.get();
         //    Vector22Writable v=labels.get(k);
        //	 v=value;
             
        // }
		
		
//		
//		Vector<ttr> ans;
//		
//		startTime=System.nanoTime();
//		for(i=0; i<hhl.query_sets.size(); i++){
//        	EdgeQuintuple eq = hhl.query_sets.get(i);
//        	long singleQueryStartTime=System.nanoTime();
//            ans = hhl.earliest_arrival_path_query(eq.u, eq.v, eq.ta, Long.MAX_VALUE-3);
//            long singleQueryEndTime=System.nanoTime();
//            double singleQueryTime=(double)(singleQueryEndTime-singleQueryStartTime)/1000;
//            System.out.println("query: "+(eq.u+1)+" to "+(eq.v+1)+" start-time "+eq.ta);
//            for(j=0;j<ans.size();j++){           	 
//            	System.out.println((ans.get(j).u+1)+" to "+(ans.get(j).v+1)+" start from: "+ans.get(j).depar_time+" take bus: "+ans.get(j).busid+" arrive at: "+ans.get(j).arrival_time);            	
//            }
//            System.out.printf("Query processing time:%.2fus\n",singleQueryTime);
//            if(j==0)
//            	System.out.print("path not found!");
//            System.out.println();
//        }
//		endTime=System.nanoTime();
//		diff=(double)(endTime-startTime)/hhl.query_sets.size()/1000;
//		System.out.printf("Average path query time:%.2fus\n",diff);
//		System.out.println(hhl.label0.get(0).m_data[0].m_data[0].toString());
//		System.out.println(hhl.label0.get(0).m_data[0].toString());
//		//TTL.HHL_temporal.TimeRange res;
//		//res=hhl.earliest_arrival_query(2, 3, 6, 99);
//		//System.out.println(res.first+" to "+res.second);
//		//ans=hhl.earliest_arrival_path_query(0,2061,19815,25625);
//		//hhl.output("D:\\myttl\\austinJava");
//		return ;
	}

}
