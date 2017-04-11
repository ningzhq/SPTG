package com.cloudyhadoop.hadoop.mapreduce;

import java.util.Arrays;
import java.util.Comparator;



public class VectorEdgeQuadruple{
	final int VectorDefaultSize = 20;
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
//		printf("iVector allocate: %d\n",n);
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
//	void push_back( const _T* p, unsigned int len )
//	{
//		while ( m_num + len > m_size )
//		{
//			re_allocate( m_size*2 );
//		}
//		memcpy( m_data+m_num, p, sizeof(_T)*len );
//		m_num += len;
//	}

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
		
//		int l,r;
//
//		for ( l = 0 , r = m_num ; l < r ; )
//		{
//			int m = (l+r)/2;
//			if ( m_data[m].compareTo(x)==1) l = m+1;
//			else r = m;
//		}
//
//		if ( l < m_num && m_data[l].compareTo(x)==0 )
//		{
//			//printf("Insert Duplicate....\n");
//            //cout<<x<<endl;
//	//		break;
//		}
//		else
//		{
//			if ( m_num > l )
//			{
//				int k=m_num;
//				while(k>=(m_num-l)){
//					m_data[k]=m_data[k-1];
//					k--;
//				}
//				//System.arraycopy(m_data,0,tmp,0,m_num);
//				//memmove( m_data+l+1, m_data+l, sizeof(_T)*(m_num-l) );
//			}
//			m_num++;
//			m_data[l] = x;
//		}
	}
	
	EdgeQuadruple get(int i) {
		// if ( i < 0 || i >= m_num )
		// {
		// printf("iVector [] out of range!!!\n");
		// }
		return m_data[i];
	}

}