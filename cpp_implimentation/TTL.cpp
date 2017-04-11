#include <cstdio>
//#include "utility1.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <queue>
#include <cassert>
#include <algorithm>
#include <map>
#include <time.h>
#include <cmath>
#include <set>
#include <queue>
#include <climits>
#include <cfloat>
#include <stack>

#include <vector>
#include <algorithm>
#include <string>
#include <ctime>
#include <cstdlib>
#include <cstring>
#include <cmath>



using namespace std;



template <typename _Key, typename _Value>
struct Key_Value
{
	_Key key;
	_Value value;
	Key_Value( const _Key& k , const _Value& v )
	{
		key = k;
		value = v;
	}
	bool operator==( const Key_Value<_Key,_Value>& p )const
	{
		return ( key == p.key );
	}
	bool operator<( const Key_Value<_Key,_Value>& p )const
	{
		if ( key < p.key ) return true;
		else if ( key > p.key ) return false;
		return ( value < p.value );
	}

	Key_Value( int tmp )
	{
		key = tmp;
		value = -1;
	}

	Key_Value()
	{
	}
};


extern const int VectorDefaultSize;

template <typename _T>
class iVector
{
public:
	unsigned int m_size;
	_T* m_data;
	unsigned int m_num;

	void free_mem()
	{
		delete[] m_data;
	}

	iVector()
	{
		//printf("%d\n",VectorDefaultSize);
		m_size = VectorDefaultSize;
		m_data = new _T[VectorDefaultSize];
		m_num = 0;
	}
	iVector( unsigned int n )
	{
		if ( n == 0 )
		{
			n = VectorDefaultSize;
		}
//		printf("iVector allocate: %d\n",n);
		m_size = n;
		m_data = new _T[m_size];
		m_num = 0;
	}
	void push_back( _T d )
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

	void re_allocate( unsigned int size )
	{
		if ( size < m_num )
		{
			return;
		}
		_T* tmp = new _T[size];
		memcpy( tmp, m_data, sizeof(_T)*m_num );
		m_size = size;
		delete[] m_data;
		m_data = tmp;
	}

	void clean()
	{
		m_num = 0;
	}


	void sorted_insert( _T& x )
	{
		if ( m_num == 0 )
		{
			push_back( x );
			return;
		}

		if ( m_num == m_size ) re_allocate( m_size*2 );

		int l,r;

		for ( l = 0 , r = m_num ; l < r ; )
		{
			int m = (l+r)/2;
			if ( m_data[m] < x ) l = m+1;
			else r = m;
		}

		if ( l < m_num && m_data[l] == x )
		{
			//printf("Insert Duplicate....\n");
            //cout<<x<<endl;
	//		break;
		}
		else
		{
			if ( m_num > l )
			{
				memmove( m_data+l+1, m_data+l, sizeof(_T)*(m_num-l) );
			}
			m_num++;
			m_data[l] = x;
		}
	}

//	bool remove_unsorted( _T& x )
//	{
//		for ( int m = 0 ; m < m_num ; ++m )
//		{
//			if ( m_data[m] == x )
//			{
//				m_num--;
//				if ( m_num > m ) memcpy( m_data+m, m_data+m+1, sizeof(_T)*(m_num-m) );
//				return true;
//			}
//		}
//		return false;
//	}

	_T& operator[]( unsigned int i )
	{
		//if ( i < 0 || i >= m_num )
		//{
		//	printf("iVector [] out of range!!!\n");
		//}
		return m_data[i];
	}
	//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	//close range check for [] in iVector if release

};



template <typename _T>
struct iMap
{
	_T* m_data;
	int m_num;
	int cur;
	iVector<int> occur;
    _T nil;
    iMap()
	{
		m_data = NULL;
		m_num = 0;
        //nil = std::make_pair((long)-9,(long)-9);
	    //nil = 1073741834;
    }
    iMap(_T tt){
        m_data = NULL;
        m_num = 0;
        nil = tt;
    }
	void free_mem()
	{
		delete[] m_data;
		occur.free_mem();
	}

	void initialize( int n )
	{
		occur.clean();
		m_num = n;
        nil = -9;
		if ( m_data != NULL )
			delete[] m_data;
		m_data = new _T[m_num];
		for ( int i = 0 ; i < m_num ; ++i )
			m_data[i] = nil;
		cur = 0;
	}
	void clean()
	{
		for ( int i = 0 ; i < occur.m_num ; ++i )
		{
			m_data[occur[i]] = nil;
		}
		occur.clean();
		cur = 0;
	}
	_T get( int p )
	{
		//if ( p < 0 || p >= m_num )
		//{
		//	printf("iMap get out of range!!!\n");
		//	return -8;
		//}
		return m_data[p];
	}
	_T& operator[](  int p )
	{
		//if ( i < 0 || i >= m_num )
		//{
		//	printf("iVector [] out of range!!!\n");
		//}
		return m_data[p];
	}
	void erase( int p )
	{
		//if ( p < 0 || p >= m_num )
		//{
		//	printf("iMap get out of range!!!\n");
		//}
		m_data[p] = nil;
		cur--;
	}
	bool notexist( int p )
	{
		return m_data[p] == nil ;
	}
	bool exist( int p )
	{
		return !(m_data[p] == nil);
	}
	void insert( int p , _T d )
	{
		//if ( p < 0 || p >= m_num )
		//{
		//	printf("iMap insert out of range!!!\n");
		//}
		if ( m_data[p] == nil )
		{
			occur.push_back( p );
			cur++;
		}
		m_data[p] = d;
	}
	void inc( int p )
	{
		//if ( m_data[p] == nil )
		//{
		//	printf("inc some unexisted point\n");
		//}
		m_data[p]++;
	}
	void inc( int p , int x )
	{
		//if ( m_data[p] == nil )
		//{
		//	printf("inc some unexisted point\n");
		//}
		m_data[p] += x;
	}
//	void dec( int p )
//	{
//		//if ( m_data[p] == nil )
//		//{
//		//	printf("dec some unexisted point\n" );
//		//}
//		m_data[p]--;
//	}
	//close range check when release!!!!!!!!!!!!!!!!!!!!
};



template <typename _Value>
struct iHeap
{
	iMap<int> pos;
	iVector<Key_Value<int,_Value> > m_data;

	void initialize( int n )
	{
		pos.initialize(n);
		m_data.re_allocate(1000);
	}

	int head()
	{
		return m_data[0].key;
	}

	void clean()
	{
		pos.occur.clean();
	}

    void free_mem()
    {
        pos.free_mem();
        m_data.free_mem();
    }
	void DeepClean()
	{
		pos.clean();
		m_data.clean();
	}

	void insert( int x, _Value y )
	{
		if ( pos.notexist(x) )
		{
			m_data.push_back( Key_Value<int,_Value>(x,y) );
			pos.insert( x , m_data.m_num-1 );
			up( m_data.m_num-1 );
		}
		else
		{
			if ( y < m_data[pos.get(x)].value )
			{
				m_data[ pos.get(x) ].value = y;
				up( pos.get(x) );
			}
			else
			{
				m_data[ pos.get(x) ].value = y;
				down( pos.get(x) );
			}
		}
	}
	int pop()
	{
		int tmp = m_data[0].key;
		pos.erase(tmp);
		if ( m_data.m_num == 1 )
		{
			m_data.m_num--;
			return tmp;
		}
		m_data[0] = m_data[m_data.m_num-1];
		m_data.m_num--;
		down(0);
		return tmp;
	}
	bool empty()
	{
		return ( m_data.m_num == 0 );
	}
	void up( int p )
	{
		Key_Value<int,_Value> x = m_data[p];
		for ( ; p > 0 && x.value < m_data[(p-1)/2].value ; p = (p-1)/2 )
		{
			m_data[p] = m_data[(p-1)/2];
			pos.insert(m_data[p].key, p);
		}
		m_data[p] = x;
		pos.insert(x.key,p);
	}
	void down( int p )
	{
		//Key_Value<int,int> x = m_data[m_data.m_num-1];
		//m_data.m_num--;

		Key_Value<int,_Value> tmp;

		for ( int i ; p < m_data.m_num ; p = i )
		{
			//m_data[p] = x;
			if ( p*2+1 < m_data.m_num && m_data[p*2+1].value < m_data[p].value )
				i = p*2+1;
			else i = p;
			if ( p*2+2 < m_data.m_num && m_data[p*2+2].value < m_data[i].value )
				i = p*2+2;
			if ( i == p ) break;

			tmp = m_data[p];
			m_data[p] = m_data[i];
			pos.insert( m_data[p].key, p );
			m_data[i] = tmp;
		}

		pos.insert(m_data[p].key,p);
	}

};


//extern int CodeEdgeRange;


//extern const int HashDefaultRelativeSize;



#define MAX_TIMESTAMP LONG_MAX-3
const int VectorDefaultSize=20;
//const int TOPNUM = 1;

struct tri
{
	int v;
	int t;
	int w;
};



template <typename __T1, typename __T2, typename __T3, typename __T4, typename __T5>
struct EdgeQuintuple
{
    __T1 u;
    __T2 v;
    __T3 ta;
    __T4 tb;
    __T5 busid;
    bool operator<(const EdgeQuintuple &e) const
    {
        if ( ta < e.ta) return true;
        else if( ta > e.ta) return false;
        else if( tb < e.tb ) return true;
        else if( tb > e.tb ) return false;
        else  if(u <e.u) return true;
        else  if(u> e.u ) return false;
        else if(v < e.v) return true;
        else if(v > e.v) return false;
        else if(busid< e.busid) return true;
        else return false;
    }
    bool operator==(const EdgeQuintuple &e)const
    {
        return u == e.u && v == e.v && ta == e.ta && tb == e.tb && busid == e.busid;
    }
    EdgeQuintuple( __T1 uu, __T2 vv, __T3 tta, __T4 ttb){
        u = uu;
        v = vv;
        ta = tta;
        tb = ttb;
    }
    EdgeQuintuple( __T1 uu, __T2 vv, __T3 tta, __T4 ttb, __T5 bbusid){
        u = uu;
        v = vv;
        ta = tta;
        tb = ttb;
        busid = bbusid;
    }
    EdgeQuintuple(){}
};

template <typename __T1, typename __T2, typename __T3, typename __T4>
struct EdgeQuadruple
{
    __T1 v;
    __T2 ta;
    __T3 tb;
    __T4 busid;

    bool operator<(const EdgeQuadruple &e) const
    {
        if ( tb < e.tb) return true;
        else if( tb > e.tb) return false;
        else if( ta < e.ta ) return true;
        else if( ta > e.ta ) return false;
        else  if(v <e.v) return true;
        else  if(v > e.v) return false;
        else if(busid < e.busid) return true;
        else  return false;
    }
    bool operator==(const EdgeQuadruple &e)const
    {
        return v == e.v && ta == e.ta && tb == e.tb && busid == e.busid;
    }
    EdgeQuadruple( __T1 vv, __T2 tta, __T3 ttb, __T4 bbusid){
        v = vv;
        ta = tta;
        tb = ttb;
        busid = bbusid;
    }
    EdgeQuadruple(){}
};



template <typename __T1, typename __T2, typename __T3, typename __T4, typename __T5, typename __T6>
struct LabelQunituple
{
    __T1 v;
    __T2 ta;
    __T3 tb;
    __T4 father;//前驱
	__T5 w;//中心点
    __T6 busid;
    int compression_type;
    LabelQunituple<__T1,__T2,__T3,__T4,__T5,__T6> *iw,*wv;
    iVector<LabelQunituple<__T1, __T2,__T3,__T4,__T5,__T6> > *list1, *list2;
    bool operator<(const LabelQunituple &e) const
    {
        if(v <e.v) return true;
        else if (v > e.v ) return false;
        else if( ta < e.ta ) return true;
        else if ( ta > e.ta ) return false;
        else if(tb < e.tb ) return true;
        else  return false;
    }
    bool operator==(const LabelQunituple &e)const
    {
        return v == e.v && ta == e.ta && tb == e.tb;
    }
    LabelQunituple( __T1 vv, __T2 tta, __T3 ttb, __T4 ffather, __T5 ww, __T6 bbusid){
        v = vv;
        ta = tta;
        tb = ttb;
        father = ffather;
		w = ww;
        busid = bbusid;
        iw = NULL;
        wv = NULL;
        list1 = NULL;
        list2 = NULL;
        compression_type = 0;
    }
    LabelQunituple(){}
};

struct ttr{
    int u;
    int v;
    int father;
    int busid;
    long depar_time;
    long arrival_time;
    int compression_type;
    LabelQunituple<int, long, long, int, int, int> *lp, *rp;
    iVector<LabelQunituple<int, long, long, int, int, int> > *llist, *rlist;
};
//struct node
//{
//	int id,w;
//	map<pair<int,int>,int>e;
//};

struct HHL_temporal
{
    int N,M,init;
    long lcnt,new_cnt;
    long tmin, tmax;//the smallest timestamp and largest timestamp in the graph
    int* vertexid2level;//obtain the level order from the vertex id;
    int* leveltovertexid;// obtain the vertex id from the level order;
    int *in_deg,*out_deg,*p,*tb,*q,*fq,*tq;
    bool compressed_or_not;
    double ordering_time;
    double indexing_time;
    int stamps;
    vector<vector<tri>>fa,ch;
	vector<vector<pair<int,int>>>cv;
	vector<int>chv;
    iVector<iVector<EdgeQuadruple<int, long, long, int> > > links[2];
    iVector<iVector<int> > static_edges[2];
    iVector<iVector<iVector<LabelQunituple<int, long, long, int, int, int> > > > label[2];
    iVector<iVector<iVector<LabelQunituple<int, long, long, int, int, int> > > > sorted_links[2];
    //iHeap<long> pq[2];
    //iHeap<long> opq;
    iMap<long> mark;
    iMap<int> busmark;
    iMap<int> fathermark;
	//map<pair<int,int>,node>dep;
	vector<pair<int,int> >id2v;
    vector<EdgeQuintuple<int,int, long, long, int> > query_sets;
    void load_temporal_graph2(string graph_name){

        clock_t load_graph_start = clock();
        FILE *file = fopen(graph_name.c_str(),"r");
		if(file==NULL)printf("error opening file %s\n",graph_name.c_str());
        //int ret_val = fscanf(file,"%d %d",&N, &M);
        int ret_val = fscanf(file,"%d",&N);
        if(ret_val<0){cout<<"error" <<endl; exit(0);}
        cout<<N<<" nodes"<<endl;

        //initialize variables dependes on the number of vertices;
        links[0].re_allocate(N);
        links[0].m_num = N;
        links[1].re_allocate(N);
        links[1].m_num = N;
        label[0].re_allocate(N);
        label[0].m_num = N;
        label[1].re_allocate(N);
        label[1].m_num = N;
        static_edges[0].re_allocate(N);
        static_edges[0].m_num = N;
        static_edges[1].re_allocate(N);
        static_edges[1].m_num = N;
       // pq[0].initialize(N);
        //pq[1].initialize(N);
       // opq.initialize(N);
        mark.initialize(N);
        busmark.initialize(N);
        fathermark.initialize(N);
        vertexid2level = new int[N];
        leveltovertexid = new int[N];
        in_deg = new int[N];
        out_deg = new int[N];
        memset(in_deg, 0x00, sizeof(int)*N);
        memset(out_deg, 0x00, sizeof(int)*N);
        tmin = MAX_TIMESTAMP;
        tmax = -1;
        int x=0, y=0,w=0,lines=0;
        long t=0;
        int busid=0;
        int num_edges =0;
//        for( ; fscanf(file, "%d", &x)==1; lines++){
//        	fscanf(file, "%d", &y);
//        	fscanf(file, "%d", &busid);
//                    for(; fscanf(file, "%d", &t)==1;){
//                        if(t==-1) break;
//                        int ret_val = fscanf(file, "%d", &w);
//                        if(ret_val<0){cout<<"error"<<endl; exit(0);}
//                        if(t< tmin)
//                            tmin = t;
//                        if( w+t > tmax)
//                            tmax = t;
//                        EdgeQuadruple<int, long, long, int> forwrad_triple(y,t,w+t, busid);
//                        EdgeQuadruple<int, long, long, int> backward_triple(x, t, w+t, busid);
//                        if( x==y){
//                            continue;
//                        }
//                        num_edges++;
//                       // static_edges[0][x].sorted_insert(y);
//                       // static_edges[1][y].sorted_insert(x);
//                        links[0][x].push_back(forwrad_triple);
//                        links[1][y].sorted_insert(backward_triple);//EdgeQuadruple的运算符重定义<tb从小到大
//                        out_deg[x]++;
//                        in_deg[y]++;
//                    }
//                }


        for( ; fscanf(file, "%d%d%d", &x,&y,&busid)==3; lines++){
        	x=x-1;
        	y=y-1;
            for(; fscanf(file, "%d", &t)==1;){
            	if(t==-1)
            		break;
            	fscanf(file, "%d",&w);

                if(t< tmin)
                    tmin = t;
                if( w+t > tmax)
                    tmax = t;
                EdgeQuadruple<int, long, long, int> forwrad_triple(y,t,w+t, busid);
                EdgeQuadruple<int, long, long, int> backward_triple(x, t, w+t, busid);
                if( x==y){
                    continue;
                }
                num_edges++;
               // static_edges[0][x].sorted_insert(y);
               // static_edges[1][y].sorted_insert(x);
                links[0][x].push_back(forwrad_triple);
                links[1][y].sorted_insert(backward_triple);//EdgeQuadruple的运算符重定义<tb从小到大
                out_deg[x]++;
                in_deg[y]++;
            }
        }

//        FILE* graph_file = fopen("D:\\myttl\\austinGraphC++.txt", "w");
//        fprintf(graph_file, "%d\n", N);
//		for (int i = 0; i < N; i++) {
//			fprintf(graph_file, "%d:", i);
//			for (int j = 0; j < links[0][i].m_num; j++)
//					fprintf(graph_file, "%d %ld %ld %d ",
//							links[0][i][j].v, links[0][i][j].ta,
//							links[0][i][j].tb, links[0][i][j].busid);
//			fprintf(graph_file, "-1 ");
//			for (int j = 0; j < links[1][i].m_num; j++)
//					fprintf(graph_file, "%d %ld %ld %d ",
//							links[1][i][j].v, links[1][i][j].ta,
//							links[1][i][j].tb ,
//							links[1][i][j].busid);
//			fprintf(graph_file, "-2\n");
//		}
//
//		fclose(graph_file);
        num_edges =0;
        for(int i=0; i<links[0].m_num; i++){
            num_edges+=links[0][i].m_num;
        }
        cout<<num_edges<<" edges"<<endl;
        fclose(file);
        clock_t load_graph_stop = clock();
        printf( "Load Graph Time: %0.2lf\n",(double)(load_graph_stop-load_graph_start)/CLOCKS_PER_SEC );

    }
    void load_temporal_graph(string graph_name){
        clock_t load_graph_start = clock();
        FILE *file = fopen(graph_name.c_str(),"r");
		if(file==NULL)printf("error opening file %s\n",graph_name.c_str());
        //int ret_val = fscanf(file,"%d %d",&N, &M);
        int ret_val = fscanf(file,"%d",&N);
        if(ret_val<0){cout<<"error" <<endl; exit(0);}
        cout<<N<<" nodes"<<endl;

        //initialize variables dependes on the number of vertices;
        links[0].re_allocate(N);
        links[0].m_num = N;
        links[1].re_allocate(N);
        links[1].m_num = N;
        label[0].re_allocate(N);
        label[0].m_num = N;
        label[1].re_allocate(N);
        label[1].m_num = N;
        static_edges[0].re_allocate(N);
        static_edges[0].m_num = N;
        static_edges[1].re_allocate(N);
        static_edges[1].m_num = N;
       // pq[0].initialize(N);
        //pq[1].initialize(N);
       // opq.initialize(N);
        mark.initialize(N);
        busmark.initialize(N);
        fathermark.initialize(N);
        vertexid2level = new int[N];
        leveltovertexid = new int[N];
        in_deg = new int[N];
        out_deg = new int[N];
        memset(in_deg, 0x00, sizeof(int)*N);
        memset(out_deg, 0x00, sizeof(int)*N);
        tmin = MAX_TIMESTAMP;
        tmax = -1;
        int x=0, y=0,w=0,lines=0;
        long t=0;
        int busid=0;
        int num_edges =0;
//        for( ; fscanf(file, "%d", &x)==1; lines++){
//        	fscanf(file, "%d", &y);
//        	fscanf(file, "%d", &busid);
//                    for(; fscanf(file, "%d", &t)==1;){
//                        if(t==-1) break;
//                        int ret_val = fscanf(file, "%d", &w);
//                        if(ret_val<0){cout<<"error"<<endl; exit(0);}
//                        if(t< tmin)
//                            tmin = t;
//                        if( w+t > tmax)
//                            tmax = t;
//                        EdgeQuadruple<int, long, long, int> forwrad_triple(y,t,w+t, busid);
//                        EdgeQuadruple<int, long, long, int> backward_triple(x, t, w+t, busid);
//                        if( x==y){
//                            continue;
//                        }
//                        num_edges++;
//                       // static_edges[0][x].sorted_insert(y);
//                       // static_edges[1][y].sorted_insert(x);
//                        links[0][x].push_back(forwrad_triple);
//                        links[1][y].sorted_insert(backward_triple);//EdgeQuadruple的运算符重定义<tb从小到大
//                        out_deg[x]++;
//                        in_deg[y]++;
//                    }
//                }


        for( ; fscanf(file, "%d:", &x)==1; lines++){
            for(; fscanf(file, "%d", &y)==1;){
                if(y==-1) break;
                int ret_val = fscanf(file, "%ld %d %d", &t, &w, &busid);
                if(ret_val<0){cout<<"error"<<endl; exit(0);}
                if(t< tmin)
                    tmin = t;
                if( w+t > tmax)
                    tmax = t;
                EdgeQuadruple<int, long, long, int> forwrad_triple(y,t,w+t, busid);
                EdgeQuadruple<int, long, long, int> backward_triple(x, t, w+t, busid);
                if( x==y){
                    continue;
                }
                num_edges++;
               // static_edges[0][x].sorted_insert(y);
               // static_edges[1][y].sorted_insert(x);
                links[0][x].push_back(forwrad_triple);
                links[1][y].sorted_insert(backward_triple);//EdgeQuadruple的运算符重定义<;tb从小到大
                out_deg[x]++;
                in_deg[y]++;
            }
        }

//        FILE* graph_file = fopen("D:\\myttl\\austinGraphC++.txt", "w");
//        fprintf(graph_file, "%d\n", N);
//		for (int i = 0; i < N; i++) {
//			fprintf(graph_file, "%d:", i);
//			for (int j = 0; j < links[0][i].m_num; j++)
//					fprintf(graph_file, "%d %ld %ld %d ",
//							links[0][i][j].v, links[0][i][j].ta,
//							links[0][i][j].tb, links[0][i][j].busid);
//			fprintf(graph_file, "-1 ");
//			for (int j = 0; j < links[1][i].m_num; j++)
//					fprintf(graph_file, "%d %ld %ld %d ",
//							links[1][i][j].v, links[1][i][j].ta,
//							links[1][i][j].tb ,
//							links[1][i][j].busid);
//			fprintf(graph_file, "-2\n");
//		}
//
//		fclose(graph_file);
        num_edges =0;
        for(int i=0; i<links[0].m_num; i++){
            num_edges+=links[0][i].m_num;
        }
        cout<<num_edges<<" edges"<<endl;
        fclose(file);
        clock_t load_graph_stop = clock();
        printf( "Load Graph Time: %0.2lf\n",(double)(load_graph_stop-load_graph_start)/CLOCKS_PER_SEC );
    }

    void generate_queries(string file_name, int num_of_queries){
        string output_filename = "query_"+file_name;
       FILE* file = fopen(output_filename.c_str(), "w");
       for(int i=0; i< num_of_queries; i++){
            int u = rand()%N;
            while(links[0][u].m_num == 0 )
                u = rand()%N;
            int v = rand()%N;
            while(links[0][v].m_num == 0 || u == v)
                v = rand()%N;
            long ta = rand()%(12*3600);
            long tb = rand() %(24*3600);
            while ( tb < ta){
                tb = rand() %(24*3600);
            }
            fprintf(file, "%d %d %ld %ld\n", u, v, ta, tb);
       }
       fclose(file);
    }

//    void add_q(int &ql,int v,int f,int t)
//    {
//        int i,j,k;
//        q[ql]=v;
//        fq[ql]=f;
//        tq[ql]=t;
//        i=ql++;
//        while(i)
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

//    void del_q(int &ql)
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

    unsigned int hash_fun(unsigned int v,unsigned int t)
    {
        return v*199999+t*9973;
    }

//    int get_ch(int u,int v,int t)
//    {
//        int r;
//        r=hash_fun(v,t)%tq[u];
//        while(1)
//        {
//            if(ch[u][r].w<0)return 0;
//            if(ch[u][r].v==v&&ch[u][r].t==t)return ch[u][r].w;
//            r++;
//            if(r==tq[u])r=0;
//        }
//    }

//    int get_fa(int u,int v,int t)
//    {
//        int r;
//        r=hash_fun(v,t)%fq[u];
//        while(1)
//        {
//            if(fa[u][r].w==-1)
//            {
//                printf("get father (%d,%d,%d) failed\n",u,v,t);
//                return -1;
//            }
//            if(fa[u][r].v==v&&fa[u][r].t==t)
//            {
//                if(fa[u][r].w<0)printf("get father (%d,%d,%d) failed\n",u,v,t);
//                return fa[u][r].w;
//            }
//            r++;
//            if(r==fa[u].size())r=0;
//        }
//    }
//
//    void add_cv(int u,int v,int s)
//    {
//        int i;
//        for(i=0;i<cv[u].size();i++)
//            if(cv[u][i].first==v)
//            {
//                cv[u][i].second+=s;
//                return;
//            }
//        cv[u].push_back(make_pair(v,s));
//    }
//
//    int add_cvt(int u,int v,int t)
//    {
//        int s=1;
//        for(int i=get_ch(u,v,t);chv[i]>=0;i++)
//            s+=add_cvt(chv[i],v,t);
//        add_cv(u,v,s);
//        return s;
//    }

//    void del_fa(int u,int v,int t)
//    {
//        int r;
//        r=hash_fun(v,t)%fq[u];
//        while(1)
//        {
//            if(fa[u][r].w==-1)
//            {
//                printf("delete father (%d,%d,%d) failed\n",u,v,t);
//                return;
//            }
//            if(fa[u][r].v==v&&fa[u][r].t==t)
//            {
//                if(fa[u][r].w<0)printf("delete father (%d,%d,%d) failed\n",u,v,t);
//                fa[u][r].w=-2;
//                q[u]--;
//                if(q[u]*3<fq[u])
//                {
//                    vector<tri>temp=fa[u];
//                    fq[u]/=2;
//                    fa[u].resize(fq[u]);
//                    for(r=0;r<fq[u];r++)
//                        fa[u][r].w=-1;
//                    for(r=0;r<temp.size();r++)
//                        if(temp[r].w>=0)
//                        {
//                            int k;
//                            for(k=hash_fun(temp[r].v,temp[r].t)%fq[u];fa[u][k].w>=0;k=((k>fq[u]-2)?0:k+1));
//                            fa[u][k]=temp[r];
//                        }
//                }
//                return;
//            }
//            r++;
//            if(r==fq[u])r=0;
//        }
//    }


    int Search_Label_Qunituple(long timestamp, iVector<LabelQunituple<int,long, long, int, int, int> >& label_list){
        if(label_list.m_num <50){
            int w =0;
            for(; w<label_list.m_num; w++){
                if(label_list[w].ta < timestamp) continue;
                break;
            }
            return w;
        }else{
            int x,y;
            int start_pos=0;
            for(x=0, y=label_list.m_num-1; x<=y;){
                int mid = x+ (y-x)/2;//do not use (x+y)/2 in case of overflow
                if(label_list[mid].ta == timestamp){
                    start_pos = mid;
                    y = mid-1;
                }
                if ( label_list[mid].ta < timestamp) x = mid+1;
                else y = mid-1;
            }
            return start_pos;
        }
    }

    int Search_Edge_Quadruple(long timestamp, iVector<EdgeQuadruple<int, long, long, int> >& edge_list, int search_case){
        //search case 0 search based on EdgeQuadruple ta value;
        if(search_case ==0){
            if(edge_list.m_num < 20){//顶点的出边少于20条，直接搜
                int w=0;
                for( ; w<edge_list.m_num; w++){
                    if(edge_list[w].ta < timestamp) continue;
                    break;
                }
                return w;
            }else{//出边多于20条，二分搜
                int x,y;
                int start_pos=0;
                for(x=0, y=edge_list.m_num-1; x<=y;){
                    int mid = x+ (y-x)/2;//do not use (x+y)/2 in case of overflow
                    if(edge_list[mid].ta == timestamp){
                        start_pos = mid;
                        y = mid-1;
                    }
                    if ( edge_list[mid].ta < timestamp) x = mid+1;
                    else y = mid-1;
                }
                return start_pos;
            }
        }else{
            //search case 1 search based on EdgeQuadruple tb value;
            if(edge_list.m_num < 20){
                int w=edge_list.m_num>0? edge_list.m_num-1: -1;
                for( ; w>=0; w--){
                    if(edge_list[w].tb > timestamp) continue;
                    break;
                }
                return w;
            }else{
                int x,y;
                int end_pos= edge_list.m_num-1;
                for(int x=0, y=edge_list.m_num-1; x<=y;){
                    int mid= x+ (y-x)/2;//do not use (x+y)/2 in case of overflow
                    if(edge_list[mid].ta == timestamp){
                        end_pos = mid;
                        x= mid+1;
                    }
                    if ( edge_list[mid].ta < timestamp) x = mid+1;
                    else y = mid-1;
                }
                return end_pos;
            }
        }
    }
    bool reachable_query(int src, int tar, long timerange_a, long timerange_b){
        src = leveltovertexid[src];
        tar = leveltovertexid[tar];
        int x,y;

        for(int i=0, j=0; i<label[0][src].m_num && j<label[1][tar].m_num;){
            iVector<LabelQunituple<int, long, long, int, int, int> >& outlabel_list = label[0][src][i];
            x = outlabel_list[0].v;
            iVector<LabelQunituple<int, long, long, int, int, int> >& inlabel_list = label[1][tar][j];
            y = inlabel_list[0].v;

            if( x < y) i++;
            else if( x > y) j++;
            else{
                long outlabel_ta, outlabel_tb;
                long inlabel_ta, inlabel_tb;
                //int k=0, m=0;
                int k = Search_Label_Qunituple(timerange_a, outlabel_list);
                int m = Search_Label_Qunituple(timerange_a, inlabel_list);
                for(; k<outlabel_list.m_num && m< inlabel_list.m_num;){
                    outlabel_ta = outlabel_list[k].ta;
                    outlabel_tb = outlabel_list[k].tb;
                    inlabel_ta = inlabel_list[m].ta;
                    inlabel_tb = inlabel_list[m].tb;
                    if(outlabel_ta < timerange_a){k++; continue;}
                    if(inlabel_ta < timerange_a){ m++; continue;}
                    if(inlabel_tb > timerange_b){
                        //i++; j++;
                        goto finished;
                        //break;
                    }
                    if(outlabel_tb <= inlabel_ta ){
                        return true;
                    }else m++;
                }
finished:
                i++;
                j++;

            }
        }
        return false;
    }

    pair<long, long> earliest_arrival_query(int src, int tar, long timerange_a, long timerange_b){
        src = leveltovertexid[src];
        tar = leveltovertexid[tar];
        pair<long, long> earliest_arrival(timerange_a, MAX_TIMESTAMP);
        int x,y;

        for(int i=0, j=0; i<label[0][src].m_num && j<label[1][tar].m_num;){
            iVector<LabelQunituple<int, long, long, int, int, int> >& outlabel_list = label[0][src][i];
            x = outlabel_list[0].v;
            iVector<LabelQunituple<int, long, long, int, int, int> >& inlabel_list = label[1][tar][j];
            y = inlabel_list[0].v;

            if( x < y) i++;
            else if( x > y) j++;
            else{
                //cout<<"fuck!!!"<<endl;
                long outlabel_ta, outlabel_tb;
                long inlabel_ta, inlabel_tb;
                //int k=0, m=0;
                int k = Search_Label_Qunituple(timerange_a, outlabel_list);
                int m = Search_Label_Qunituple(timerange_a, inlabel_list);
                for(; k<outlabel_list.m_num && m< inlabel_list.m_num;){
                    outlabel_ta = outlabel_list[k].ta;
                    outlabel_tb = outlabel_list[k].tb;
                    inlabel_ta = inlabel_list[m].ta;
                    inlabel_tb = inlabel_list[m].tb;
                    if(outlabel_ta < timerange_a){k++; continue;}
                    if(inlabel_ta < timerange_a){ m++; continue;}
                    if(inlabel_tb > timerange_b){
                        //i++; j++;
                        goto finished;
                        //break;
                    }
                    if(outlabel_tb <= inlabel_ta ){
                    	///if(earliest_arrival.second > inlabel_tb){
                        if(earliest_arrival.second >= inlabel_tb){
                        	if(earliest_arrival.first>=outlabel_ta){//两个if“==”时表示找到两条时间区间一样的路径，取第一条
                        		k++;
                        	    continue;
                        	}

                        	earliest_arrival.first = outlabel_ta;
                        	earliest_arrival.second = inlabel_tb;
                        }

                       /// goto finished;
                        k++;
                    }else m++;
                }
finished:
                i++;
                j++;

            }
        }
        return earliest_arrival;

    }

    pair<long, long> earliest_arrival_query(int src, int tar, long timerange_a, long timerange_b, int& mid_point,
                                            int& src_child, int& tar_father,
                                            long& mid_point_arrival, long& mid_point_departure,
                                            int& first_bus, int& second_bus,
                                            ttr& sca, ttr& scb){
        //src = leveltovertexid[src];
        //tar = leveltovertexid[tar];
        //this will only be called by path_query, and it already change src vertex id to its order
        pair<long, long> earliest_arrival(timerange_a, MAX_TIMESTAMP);
        int x,y;

        for(int i=0, j=0; i<label[0][src].m_num && j<label[1][tar].m_num;){
            iVector<LabelQunituple<int, long, long, int, int, int> >& outlabel_list = label[0][src][i];
            x = outlabel_list[0].v;
            iVector<LabelQunituple<int, long, long, int, int, int> >& inlabel_list = label[1][tar][j];
            y = inlabel_list[0].v;

            if( x < y) i++;
            else if( x > y) j++;
            else{
                //cout<<"fuck!!!"<<endl;
                long outlabel_ta, outlabel_tb;
                long inlabel_ta, inlabel_tb;
                //int k=0, m=0;
                int k = Search_Label_Qunituple(timerange_a, outlabel_list);
                int m = Search_Label_Qunituple(timerange_a, inlabel_list);
                for(; k<outlabel_list.m_num && m< inlabel_list.m_num;){
                    outlabel_ta = outlabel_list[k].ta;
                    outlabel_tb = outlabel_list[k].tb;
                    inlabel_ta = inlabel_list[m].ta;
                    inlabel_tb = inlabel_list[m].tb;
                    if(outlabel_ta < timerange_a){k++; continue;}
                    if(inlabel_ta < timerange_a){ m++; continue;}
                    if(inlabel_tb > timerange_b){
                        //i++; j++;
                        goto finished;
                        //break;
                    }
                    if(outlabel_tb <= inlabel_ta ){
                       // if(earliest_arrival.second > inlabel_tb){
                    	if(earliest_arrival.second >= inlabel_tb){//
                    		if(earliest_arrival.first>=outlabel_ta){//两个if“==”时表示找到两条时间区间一样的路径，取第一条
                    			k++;
								continue;
                    		}
                            earliest_arrival.first = outlabel_ta;
                            earliest_arrival.second = inlabel_tb;
                            mid_point = x;
                            mid_point_arrival = outlabel_tb;
                            mid_point_departure = inlabel_ta;
                            src_child = outlabel_list[k].w;
                            tar_father = inlabel_list[m].w;
                            first_bus = outlabel_list[k].busid;
                            second_bus = inlabel_list[m].busid;
                            sca.lp = outlabel_list[k].iw;
                            sca.rp = outlabel_list[k].wv;
                            scb.lp = inlabel_list[m].iw;
                            scb.rp = inlabel_list[m].wv;
                        }
                        ///goto finished;
                        k++;
                        //k++;
                    }else
                    	m++;

                }
finished:
                i++;
                j++;

            }
        }
        return earliest_arrival;

    }


    vector<ttr> earliest_arrival_path_query(int src, int tar, long timerange_a, long timerange_b){
        vector<ttr> path;
        stack<ttr> shortcut_stack;
        if(src == tar){
            return path;
        }
        if(src<0||src>N||tar<0||tar>N)
        	return path;
        src = leveltovertexid[src];
        tar = leveltovertexid[tar];
        ttr lh_shortcut, rh_shortcut;
        lh_shortcut.father = -1;
        rh_shortcut.father = -1;
        lh_shortcut.lp = NULL;
        lh_shortcut.rp = NULL;
        rh_shortcut.lp = NULL;
        rh_shortcut.rp = NULL;
        int mid_point=-1;
        pair<long, long> duration = earliest_arrival_query(src, tar, timerange_a, timerange_b, mid_point,
                                                           lh_shortcut.father, rh_shortcut.father, lh_shortcut.arrival_time, rh_shortcut.depar_time,
                                                           lh_shortcut.busid, rh_shortcut.busid,lh_shortcut, rh_shortcut );
        if(duration.second == MAX_TIMESTAMP) return path;
        assert(mid_point!=-1);
        lh_shortcut.u = src; lh_shortcut.v = mid_point; lh_shortcut.depar_time = duration.first;
        rh_shortcut.u = mid_point; rh_shortcut.v = tar; rh_shortcut.arrival_time = duration.second;
        shortcut_stack.push(rh_shortcut);
        shortcut_stack.push(lh_shortcut);
        while(!shortcut_stack.empty()){
            ttr top_shortcut = shortcut_stack.top();
            shortcut_stack.pop();
            if(top_shortcut.u == top_shortcut.v|| top_shortcut.u == top_shortcut.father ||
               top_shortcut.v == top_shortcut.father){
                continue;
            }
            else if(top_shortcut.father <0){
                top_shortcut.u = vertexid2level[top_shortcut.u];
                top_shortcut.v = vertexid2level[top_shortcut.v];
                path.push_back(top_shortcut);
            }else{
               ttr lh_shortcut, rh_shortcut;
               lh_shortcut.u = top_shortcut.u;
               lh_shortcut.depar_time = top_shortcut.depar_time;
               lh_shortcut.arrival_time = -1;
               lh_shortcut.v = top_shortcut.father;
               lh_shortcut.father = -1;
               lh_shortcut.lp = NULL;
               lh_shortcut.rp = NULL;

               rh_shortcut.u = top_shortcut.father;
               rh_shortcut.v = top_shortcut.v;
               rh_shortcut.arrival_time = top_shortcut.arrival_time;
               rh_shortcut.father = -1;
               rh_shortcut.depar_time = -1;
               rh_shortcut.lp = NULL;
               rh_shortcut.rp = NULL;

               if(top_shortcut.rp != NULL){
                   LabelQunituple<int,long, long, int, int, int> sc2 = *(top_shortcut.rp);
                   rh_shortcut.father = sc2.w;
                   rh_shortcut.depar_time = sc2.ta;
                   ///rh_shortcut.depar_time = sc2.tb;
                   rh_shortcut.busid = sc2.busid;
                   rh_shortcut.lp = sc2.iw;
                   rh_shortcut.rp = sc2.wv;
                   shortcut_stack.push(rh_shortcut);
               }

               if(top_shortcut.lp != NULL){
                   LabelQunituple<int,long, long, int, int, int> sc1 = *(top_shortcut.lp);
                   lh_shortcut.father = sc1.w;
                   lh_shortcut.arrival_time = sc1.tb;
                   lh_shortcut.busid = sc1.busid;
                   lh_shortcut.lp = sc1.iw;
                   lh_shortcut.rp = sc1.wv;
                   shortcut_stack.push(lh_shortcut);
               }
            }
        }

        return path;
    }

    vector<ttr> earliest_arrival_concise_path_query(int src, int tar, long timerange_a, long timerange_b){
            vector<ttr> path;
            stack<ttr> shortcut_stack;
            if(src == tar){
                return path;
            }
            if(src<0||src>N||tar<0||tar>N)
            	return path;
            src = leveltovertexid[src];
            tar = leveltovertexid[tar];
            ttr lh_shortcut, rh_shortcut;
            lh_shortcut.father = -1;
            rh_shortcut.father = -1;
            lh_shortcut.lp = NULL;
            lh_shortcut.rp = NULL;
            rh_shortcut.lp = NULL;
            rh_shortcut.rp = NULL;
            int mid_point=-1;
            pair<long, long> duration = earliest_arrival_query(src, tar, timerange_a, timerange_b, mid_point,
                                                               lh_shortcut.father, rh_shortcut.father, lh_shortcut.arrival_time, rh_shortcut.depar_time,
                                                               lh_shortcut.busid, rh_shortcut.busid,lh_shortcut, rh_shortcut );
            if(duration.second == MAX_TIMESTAMP) return path;
            assert(mid_point!=-1);
            lh_shortcut.u = src; lh_shortcut.v = mid_point; lh_shortcut.depar_time = duration.first;
            rh_shortcut.u = mid_point; rh_shortcut.v = tar; rh_shortcut.arrival_time = duration.second;
            shortcut_stack.push(rh_shortcut);
            shortcut_stack.push(lh_shortcut);
            ///查路径时下面代码改成了不展开bus相同的每一条边，即所谓的concise_path
            int preBus=-2;
            while(!shortcut_stack.empty()){
                ttr top_shortcut = shortcut_stack.top();
                shortcut_stack.pop();
                if(top_shortcut.u == top_shortcut.v|| top_shortcut.u == top_shortcut.father ||
                   top_shortcut.v == top_shortcut.father){
                    continue;
                }
                else if(top_shortcut.father <0||top_shortcut.busid!=-1){//top_shortcut.busid!=-1表示直达路径不用展开了
                    top_shortcut.u = vertexid2level[top_shortcut.u];
                    top_shortcut.v = vertexid2level[top_shortcut.v];
                    if(top_shortcut.busid==preBus){
                    	path.back().arrival_time=top_shortcut.arrival_time;
                    	path.back().v=top_shortcut.v;
                    }else{
                    	path.push_back(top_shortcut);
                    	preBus=top_shortcut.busid;
                    }

                }else if(top_shortcut.busid==-1){
                   ttr lh_shortcut, rh_shortcut;
                   lh_shortcut.u = top_shortcut.u;
                   lh_shortcut.depar_time = top_shortcut.depar_time;
                   lh_shortcut.arrival_time = -1;
                   lh_shortcut.v = top_shortcut.father;
                   lh_shortcut.father = -1;
                   lh_shortcut.lp = NULL;
                   lh_shortcut.rp = NULL;

                   rh_shortcut.u = top_shortcut.father;
                   rh_shortcut.v = top_shortcut.v;
                   rh_shortcut.arrival_time = top_shortcut.arrival_time;
                   rh_shortcut.father = -1;
                   rh_shortcut.depar_time = -1;
                   rh_shortcut.lp = NULL;
                   rh_shortcut.rp = NULL;

                   if(top_shortcut.rp != NULL){
                       LabelQunituple<int,long, long, int, int, int> sc2 = *(top_shortcut.rp);
                       rh_shortcut.father = sc2.w;
                       rh_shortcut.depar_time = sc2.ta;
                       ///rh_shortcut.depar_time = sc2.tb;
                       rh_shortcut.busid = sc2.busid;
                       rh_shortcut.lp = sc2.iw;
                       rh_shortcut.rp = sc2.wv;
                       shortcut_stack.push(rh_shortcut);
                   }

                   if(top_shortcut.lp != NULL){
                       LabelQunituple<int,long, long, int, int, int> sc1 = *(top_shortcut.lp);
                       lh_shortcut.father = sc1.w;
                       lh_shortcut.arrival_time = sc1.tb;
                       lh_shortcut.busid = sc1.busid;
                       lh_shortcut.lp = sc1.iw;
                       lh_shortcut.rp = sc1.wv;
                       shortcut_stack.push(lh_shortcut);
                   }
                }
            }

            return path;
        }





    pair<long, long> latest_departure_query(int src, int tar, long timerange_a, long timerange_b){
        src = leveltovertexid[src];
        tar = leveltovertexid[tar];
        pair<long, long> latest_departure(-(MAX_TIMESTAMP), timerange_b);
        int x,y;

        for(int i=0, j=0; i<label[0][src].m_num && j<label[1][tar].m_num;){
            iVector<LabelQunituple<int, long, long, int, int, int> >& outlabel_list = label[0][src][i];
            x = outlabel_list[0].v;
            iVector<LabelQunituple<int, long, long, int, int, int> >& inlabel_list = label[1][tar][j];
            y = inlabel_list[0].v;

            if( x < y) i++;
            else if( x > y) j++;
            else{
                long outlabel_ta, outlabel_tb;
                long inlabel_ta, inlabel_tb;
                int k=0, m=0;
                //int k = Search_Label_Qunituple(timerange_a, outlabel_list);
                //int m = Search_Label_Qunituple(timerange_a, inlabel_list);
                for(; k<outlabel_list.m_num && m< inlabel_list.m_num;){
                    outlabel_ta = outlabel_list[k].ta;
                    outlabel_tb = outlabel_list[k].tb;
                    inlabel_ta = inlabel_list[m].ta;
                    inlabel_tb = inlabel_list[m].tb;
                    if(outlabel_ta < timerange_a){k++; continue;}
                    if(inlabel_ta < timerange_a){ m++; continue;}
                    if(inlabel_tb > timerange_b){
                        //i++; j++;
                        goto finished;
                        //break;
                    }
                    if(outlabel_tb <= inlabel_ta ){
                        if(latest_departure.first < outlabel_ta){
                            latest_departure.first = outlabel_ta;
                            latest_departure.second = inlabel_tb;
                        }
                        k++;
                    }else m++;
                }
finished:
                i++;
                j++;
            }
        }
        return latest_departure;
    }

    pair<long, long> latest_departure_query(int src, int tar, long timerange_a, long timerange_b, int& mid_point,
                                            int& src_child, int& tar_father,
                                            long& mid_point_arrival, long& mid_point_departure,
                                            int& first_bus, int& second_bus,
                                            ttr& sca, ttr& scb){
        //src = leveltovertexid[src];
        //tar = leveltovertexid[tar];
        pair<long, long> latest_departure(-(MAX_TIMESTAMP), timerange_b);
        int x,y;

        for(int i=0, j=0; i<label[0][src].m_num && j<label[1][tar].m_num;){
            iVector<LabelQunituple<int, long, long, int, int, int> >& outlabel_list = label[0][src][i];
            x = outlabel_list[0].v;
            iVector<LabelQunituple<int, long, long, int, int, int> >& inlabel_list = label[1][tar][j];
            y = inlabel_list[0].v;

            if( x < y) i++;
            else if( x > y) j++;
            else{
                long outlabel_ta, outlabel_tb;
                long inlabel_ta, inlabel_tb;
                int k=0, m=0;
                //int k = Search_Label_Qunituple(timerange_a, outlabel_list);
                //int m = Search_Label_Qunituple(timerange_a, inlabel_list);
                for(; k<outlabel_list.m_num && m< inlabel_list.m_num;){
                    outlabel_ta = outlabel_list[k].ta;
                    outlabel_tb = outlabel_list[k].tb;
                    inlabel_ta = inlabel_list[m].ta;
                    inlabel_tb = inlabel_list[m].tb;
                    if(outlabel_ta < timerange_a){k++; continue;}
                    if(inlabel_ta < timerange_a){ m++; continue;}
                    if(inlabel_tb > timerange_b){
                        //i++; j++;
                        goto finished;
                        //break;
                    }
                    if(outlabel_tb <= inlabel_ta ){
                        if(latest_departure.first < outlabel_ta){
                            latest_departure.first = outlabel_ta;
                            latest_departure.second = inlabel_tb;
                            mid_point_arrival = outlabel_tb;
                            mid_point_departure = inlabel_ta;
                            mid_point = x;
                            src_child = outlabel_list[k].w;
                            tar_father = inlabel_list[m].w;
                            first_bus = outlabel_list[k].busid;
                            second_bus = inlabel_list[m].busid;
                            sca.lp = outlabel_list[k].iw;
                            sca.rp = outlabel_list[k].wv;
                            scb.lp = inlabel_list[m].iw;
                            scb.rp = inlabel_list[m].wv;
                        }
                        k++;
                    }else m++;

                }
finished:
                i++;
                j++;
            }
        }
        return latest_departure;
    }
    vector<ttr> latest_departure_path_query(int src, int tar, long timerange_a, long timerange_b){
        vector<ttr> path;
        stack<ttr> shortcut_stack;
        if(src == tar){
            return path;
        }
        src = leveltovertexid[src];
        tar = leveltovertexid[tar];
        ttr lh_shortcut, rh_shortcut;
        lh_shortcut.father = -1;
        rh_shortcut.father = -1;
        lh_shortcut.lp = NULL;
        lh_shortcut.rp = NULL;
        rh_shortcut.lp = NULL;
        rh_shortcut.rp = NULL;
        int mid_point=-1;
        pair<long, long> duration = earliest_arrival_query(src, tar, timerange_a, timerange_b, mid_point,
                                                           lh_shortcut.father, rh_shortcut.father, lh_shortcut.arrival_time, rh_shortcut.depar_time,
                                                           lh_shortcut.busid, rh_shortcut.busid,lh_shortcut, rh_shortcut );
        if(duration.second == MAX_TIMESTAMP) return path;
        assert(mid_point!=-1);
        lh_shortcut.u = src; lh_shortcut.v = mid_point; lh_shortcut.depar_time = duration.first;
        rh_shortcut.u = mid_point; rh_shortcut.v = tar; rh_shortcut.arrival_time = duration.second;
        shortcut_stack.push(rh_shortcut);
        shortcut_stack.push(lh_shortcut);
        while(!shortcut_stack.empty()){
            ttr top_shortcut = shortcut_stack.top();
            shortcut_stack.pop();
            if(top_shortcut.u == top_shortcut.v|| top_shortcut.u == top_shortcut.father ||
               top_shortcut.v == top_shortcut.father){
                continue;
            }
            else if(top_shortcut.u!= top_shortcut.v && top_shortcut.father <0){
                top_shortcut.u = vertexid2level[top_shortcut.u];
                top_shortcut.v = vertexid2level[top_shortcut.v];
                path.push_back(top_shortcut);
            }else{
               ttr lh_shortcut, rh_shortcut;
               lh_shortcut.u = top_shortcut.u;
               lh_shortcut.depar_time = top_shortcut.depar_time;
               lh_shortcut.arrival_time = -1;
               lh_shortcut.v = top_shortcut.father;
               lh_shortcut.father = -1;
               lh_shortcut.lp = NULL;
               lh_shortcut.rp = NULL;

               rh_shortcut.u = top_shortcut.father;
               rh_shortcut.v = top_shortcut.v;
               rh_shortcut.arrival_time = top_shortcut.arrival_time;
               rh_shortcut.father = -1;
               rh_shortcut.depar_time = -1;
               rh_shortcut.lp = NULL;
               rh_shortcut.rp = NULL;

               if(top_shortcut.rp != NULL){
                   LabelQunituple<int,long, long, int, int, int> sc2 = *(top_shortcut.rp);
                   rh_shortcut.father = sc2.w;
                   rh_shortcut.depar_time = sc2.tb;
                   rh_shortcut.busid = sc2.busid;
                   rh_shortcut.lp = sc2.iw;
                   rh_shortcut.rp = sc2.wv;
                   shortcut_stack.push(rh_shortcut);
               }

               if(top_shortcut.lp != NULL){
                   LabelQunituple<int,long, long, int, int, int> sc1 = *(top_shortcut.lp);
                   lh_shortcut.father = sc1.w;
                   lh_shortcut.arrival_time = sc1.tb;
                   lh_shortcut.busid = sc1.busid;
                   lh_shortcut.lp = sc1.iw;
                   lh_shortcut.rp = sc1.wv;
                   shortcut_stack.push(lh_shortcut);
               }
            }
        }
        return path;
    }

    pair<long,long> fastest_duration_query(int src, int tar, long timerange_a, long timerange_b){
        src = leveltovertexid[src];
        tar = leveltovertexid[tar];
        pair<long, long> fastest_duration(-1, MAX_TIMESTAMP);
        int x,y;

        for(int i=0, j=0; i<label[0][src].m_num && j<label[1][tar].m_num;){
            iVector<LabelQunituple<int, long, long, int, int, int> >& outlabel_list = label[0][src][i];
            x = outlabel_list[0].v;
            iVector<LabelQunituple<int, long, long, int, int, int> >& inlabel_list = label[1][tar][j];
            y = inlabel_list[0].v;

            if( x < y) i++;
            else if( x > y) j++;
            else{
                long outlabel_ta, outlabel_tb;
                long inlabel_ta, inlabel_tb;
                int k=0, m=0;
                //int k = Search_Label_Qunituple(timerange_a, outlabel_list);
                //int m = Search_Label_Qunituple(timerange_a, inlabel_list);
                for(; k<outlabel_list.m_num && m< inlabel_list.m_num;){
                    outlabel_ta = outlabel_list[k].ta;
                    outlabel_tb = outlabel_list[k].tb;
                    inlabel_ta = inlabel_list[m].ta;
                    inlabel_tb = inlabel_list[m].tb;
                    if(outlabel_ta < timerange_a){k++; continue;}
                    if(inlabel_ta < timerange_a){ m++; continue;}
                    if(inlabel_tb > timerange_b){
                        //i++; j++;
                        goto finished;
                        //break;
                    }
                    if(outlabel_tb <= inlabel_ta ){
                        long duration = inlabel_tb - outlabel_ta;
                        if(fastest_duration.second - fastest_duration.first > duration){
                            fastest_duration.first = outlabel_ta;
                            fastest_duration.second = inlabel_tb;
                        }
                        k++;
                    }else m++;
                }
finished:
                i++;
                j++;

            }
        }
        return fastest_duration;
    }

    pair<long,long> fastest_duration_query(int src, int tar, long timerange_a, long timerange_b, int& mid_point,
                                           int& src_child, int& tar_father, long &mid_point_arrival,
                                           long &mid_point_departure, int& first_bus, int& second_bus,
                                           ttr& sca, ttr& scb){
        //this will only be called by path_query, and it already change src vertex id to its order
        //src = leveltovertexid[src];
        //tar = leveltovertexid[tar];
        pair<long, long> fastest_duration(-1, MAX_TIMESTAMP);
        int x,y;
        for(int i=0, j=0; i<label[0][src].m_num && j<label[1][tar].m_num;){
            iVector<LabelQunituple<int, long, long, int, int, int> >& outlabel_list = label[0][src][i];
            x = outlabel_list[0].v;
            iVector<LabelQunituple<int, long, long, int, int, int> >& inlabel_list = label[1][tar][j];
            y = inlabel_list[0].v;

            if( x < y) i++;
            else if( x > y) j++;
            else{
                long outlabel_ta, outlabel_tb;
                long inlabel_ta, inlabel_tb;
                int k=0, m=0;
                //int k = Search_Label_Qunituple(timerange_a, outlabel_list);
                //int m = Search_Label_Qunituple(timerange_a, inlabel_list);
                for(; k<outlabel_list.m_num && m< inlabel_list.m_num;){
                    outlabel_ta = outlabel_list[k].ta;
                    outlabel_tb = outlabel_list[k].tb;
                    inlabel_ta = inlabel_list[m].ta;
                    inlabel_tb = inlabel_list[m].tb;
                    if(outlabel_ta < timerange_a){k++; continue;}
                    if(inlabel_ta < timerange_a){ m++; continue;}
                    if(inlabel_tb > timerange_b){
                        //i++; j++;
                        goto finished;
                        //break;
                    }
                    if(outlabel_tb <= inlabel_ta ){
                        long duration = inlabel_tb - outlabel_ta;
                        if(fastest_duration.second - fastest_duration.first > duration){
                            fastest_duration.first = outlabel_ta;
                            fastest_duration.second = inlabel_tb;
                            mid_point = x;
                            mid_point_arrival = outlabel_tb;
                            mid_point_departure = inlabel_ta;
                            sca.lp = outlabel_list[k].iw;
                            sca.rp = outlabel_list[k].wv;
                            src_child = outlabel_list[k].w;
                            first_bus = outlabel_list[k].busid;
                            tar_father = inlabel_list[m].w;
                            second_bus = inlabel_list[m].busid;
                            scb.lp = inlabel_list[m].iw;
                            scb.rp = inlabel_list[m].wv;
                        }
                        k++;
                    }else m++;
                }
finished:
                i++;
                j++;
            }
        }
        return fastest_duration;
    }

    vector<ttr> fastest_path_query(int src, int tar, long timerange_a, long timerange_b){
        vector<ttr> path;
        stack<ttr> shortcut_stack;
        if(src == tar){
            return path;
        }
        src = leveltovertexid[src];
        tar = leveltovertexid[tar];
        ttr lh_shortcut, rh_shortcut;
        lh_shortcut.father = -1;
        rh_shortcut.father = -1;
        lh_shortcut.lp = NULL;
        lh_shortcut.rp = NULL;
        rh_shortcut.lp = NULL;
        rh_shortcut.rp = NULL;
        int mid_point=-1;
        pair<long, long> duration = fastest_duration_query(src, tar, timerange_a, timerange_b, mid_point,
                                                           lh_shortcut.father, rh_shortcut.father, lh_shortcut.arrival_time, rh_shortcut.depar_time,
                                                           lh_shortcut.busid, rh_shortcut.busid,lh_shortcut, rh_shortcut );
        if(duration.second == MAX_TIMESTAMP) return path;
        assert(mid_point!=-1);
        lh_shortcut.u = src; lh_shortcut.v = mid_point; lh_shortcut.depar_time = duration.first;
        rh_shortcut.u = mid_point; rh_shortcut.v = tar; rh_shortcut.arrival_time = duration.second;
        shortcut_stack.push(rh_shortcut);
        shortcut_stack.push(lh_shortcut);
        while(!shortcut_stack.empty()){
            ttr top_shortcut = shortcut_stack.top();
            shortcut_stack.pop();
            if(top_shortcut.u == top_shortcut.v|| top_shortcut.u == top_shortcut.father ||
               top_shortcut.v == top_shortcut.father){
                continue;
            }
            else if( top_shortcut.father <0){///top_shortcut.u!= top_shortcut.v &&top_shortcut.father <0
                top_shortcut.u = vertexid2level[top_shortcut.u];
                top_shortcut.v = vertexid2level[top_shortcut.v];
                path.push_back(top_shortcut);
            }else{
               ttr lh_shortcut, rh_shortcut;
               lh_shortcut.u = top_shortcut.u;
               lh_shortcut.depar_time = top_shortcut.depar_time;
               lh_shortcut.arrival_time = -1;
               lh_shortcut.v = top_shortcut.father;
               lh_shortcut.father = -1;
               lh_shortcut.lp = NULL;
               lh_shortcut.rp = NULL;

               rh_shortcut.u = top_shortcut.father;
               rh_shortcut.v = top_shortcut.v;
               rh_shortcut.arrival_time = top_shortcut.arrival_time;
               rh_shortcut.father = -1;
               rh_shortcut.depar_time = -1;
               rh_shortcut.lp = NULL;
               rh_shortcut.rp = NULL;

               if(top_shortcut.rp != NULL){
                   LabelQunituple<int,long, long, int, int, int> sc2 = *(top_shortcut.rp);
                   rh_shortcut.father = sc2.w;
                   ///rh_shortcut.depar_time = sc2.tb;
                   rh_shortcut.depar_time = sc2.ta;
                   rh_shortcut.busid = sc2.busid;
                   rh_shortcut.lp = sc2.iw;
                   rh_shortcut.rp = sc2.wv;
                   shortcut_stack.push(rh_shortcut);
               }

               if(top_shortcut.lp != NULL){
                   LabelQunituple<int,long, long, int, int, int> sc1 = *(top_shortcut.lp);
                   lh_shortcut.father = sc1.w;
                   lh_shortcut.arrival_time = sc1.tb;
                   lh_shortcut.busid = sc1.busid;
                   lh_shortcut.lp = sc1.iw;
                   lh_shortcut.rp = sc1.wv;
                   shortcut_stack.push(lh_shortcut);
               }
            }
        }
        return path;
    }


    void add_Label(iVector< iVector<LabelQunituple<int, long, long, int, int, int> > > & insert_list, LabelQunituple<int,long, long, int, int, int> &new_label){
        int last_list_index = insert_list.m_num -1;
        if(last_list_index>=0 && insert_list[last_list_index][0].v == new_label.v){
            insert_list[last_list_index].sorted_insert(new_label);
        }else{
            iVector< LabelQunituple<int, long, long, int, int, int> > new_list;
            new_list.sorted_insert(new_label);
            insert_list.push_back(new_list);
        }
    }

    void func(){
    	for(int i=0; i<N; i++){
    	            leveltovertexid[vertexid2level[i]] = i;
    	        }
    }

    void compute_index_with_order_BFS(){
        clock_t start_index_construction = clock();
		int *w_=new int[N];
		lcnt=0;
        func();
        iHeap<long> nodes_pq[2];
        nodes_pq[0].initialize(N);
        nodes_pq[1].initialize(N);
        for(int i=0; i<N; i++){
            int v = vertexid2level[i];

            //forward search
            mark.clean();
            for(int k=links[0][v].m_num-1; k>=0; k--){//
				memset(w_,-1,N*4);
				printf("%d",leveltovertexid[links[0][v][k].v]);
				if(leveltovertexid[links[0][v][k].v]<=i)continue;//links[v][k]中的k是顶点v的第k条出边，这条出边指向的顶点id是links[v][k].v
                long start_time = links[0][v][k].ta;//从出发时间最大开始找
                mark.insert(v, start_time);
                for( ; k>=0 && start_time == links[0][v][k].ta ; k--){//找同一起点出发时间相同的边
                    int same_timestamp_vertex = links[0][v][k].v;
					if(leveltovertexid[same_timestamp_vertex]<=i)continue;
                    long ts = links[0][v][k].tb;
                    mark.insert(same_timestamp_vertex,  ts);
                    busmark.insert(same_timestamp_vertex, links[0][v][k].busid);
                    fathermark.insert(same_timestamp_vertex, v);
                    nodes_pq[0].insert(same_timestamp_vertex, ts-start_time);
					w_[same_timestamp_vertex]=-2;
                }
                k++;//for循环跳出时减多了一次k
                LabelQunituple<int , long, long, int, int, int> p(leveltovertexid[v], start_time, start_time, -1, -2, -1);
                add_Label( label[0][leveltovertexid[v]], p );
                //label[0][leveltovertexid[v]].sorted_insert(p);
                while(!nodes_pq[0].empty()){
                    int u = nodes_pq[0].head();
                    nodes_pq[0].pop();
                    long query_range_start = start_time, query_range_end;
                    if(mark.exist(u)){
                        query_range_end = mark.m_data[u];
                    }else{
                        query_range_end = MAX_TIMESTAMP;
                    }
                    bool reachable = reachable_query(v, u, query_range_start, query_range_end);
                    if(reachable){
                        if(mark.exist(u))
                            mark.erase(u);
                        continue;//!!!!!!!
                    }
                    long arrival_u_time = query_range_end;
                    for(int j=Search_Edge_Quadruple(arrival_u_time, links[0][u], 0); j < links[0][u].m_num; j++){
                        // use binary search to get correct position
                        if(links[0][u][j].ta <arrival_u_time) continue;
                        int w = links[0][u][j].v;
						if(leveltovertexid[w]<=i)continue;
                        long taa = links[0][u][j].ta;
                        long tbb = links[0][u][j].tb;
                        int busid = links[0][u][j].busid;
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
							&&(w_[w]==-1||w_[father]>=0&&w_[father]<w_[w]||leveltovertexid[father]<w_[w])){
                            mark.insert(w,  tbb);
                            nodes_pq[0].insert(w, tbb - start_time);
                            if(busmark.m_data[father] == busid) busmark.insert(w, busid);
                            else busmark.insert(w, -1);
                            fathermark.insert(w, father);
//							if(tbb-start_time==w_end_time-w_start_time)printf("%d",w_[w]);
							w_[w]=w_[father];
							if(w==1590){
								int xx;
								xx=1;
							}
							if(w_[w]<0||leveltovertexid[father]<w_[w])
								w_[w]=leveltovertexid[father];
//							if(tbb-start_time==w_end_time-w_start_time)printf(" %d\n",w_[w]);
                        }
                    }
                }

                for( int counter =0; counter <mark.occur.m_num; counter++){
                    if(mark.exist(mark.occur[counter])){
                        int j = mark.occur[counter];
                        if(j!=v){
                            long ta = start_time;
                            long tb = mark.m_data[j];
                            int father = fathermark.m_data[j];
                            int busid = busmark.m_data[j];
                            assert(father>=0);
                            LabelQunituple<int, long, long, int, int, int> new_p(leveltovertexid[v],ta, tb, leveltovertexid[father], w_[j], busid);
                            //label[1][leveltovertexid[j]].sorted_insert(new_p);
                            add_Label(label[1][leveltovertexid[j]], new_p);
//							printf("%d in: %d %d %d %d %d\n",j,v,ta,tb,father,busid);
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
            for(int k=0; k<links[1][v].m_num; k++){
				memset(w_,-1,N*4);
                long end_time =  links[1][v][k].tb;
                mark.insert(v, end_time );
                for( ; k< links[1][v].m_num && end_time == links[1][v][k].tb ; k++){
                    int same_timestamp_vertex = links[1][v][k].v;
                    if(leveltovertexid[same_timestamp_vertex]<=i)continue;
                    long ts = links[1][v][k].ta;
                    int busid = links[1][v][k].busid;
                    mark.insert(same_timestamp_vertex, ts );
                    busmark.insert(same_timestamp_vertex, busid);
                    fathermark.insert(same_timestamp_vertex, v);
                    nodes_pq[1].insert(same_timestamp_vertex, end_time -ts);
					w_[same_timestamp_vertex]=-2;
                }
                k--;
                LabelQunituple<int , long, long, int, int, int> p(leveltovertexid[v], links[1][v][k].tb, links[1][v][k].tb, -1, -2, -1);
                //label[1][leveltovertexid[v]].sorted_insert(p);
                add_Label(label[1][leveltovertexid[v]], p);

                while(!nodes_pq[1].empty()){
                    int u = nodes_pq[1].head();
                    nodes_pq[1].pop();
                    long query_range_start, query_range_end=end_time;
                    if( mark.exist(u)){
                        query_range_start = mark.m_data[u];
                    }else{
                        query_range_start = -1;
                    }
                    bool reachable = reachable_query(u,v, query_range_start, query_range_end);
                    if(reachable){
                        if(mark.exist(u))
                            mark.erase(u);
                        continue;
                    }
                    long start_from_u_time = query_range_start;
                    for(int j=Search_Edge_Quadruple(start_from_u_time, links[1][u], 1); j >=0; j--){
                        //use binary search to get correct position
                        if(links[1][u][j].tb > start_from_u_time ) continue;
                        int w = links[1][u][j].v;
                        if(leveltovertexid[w]<=i)continue;
                        long taa = links[1][u][j].ta;
                        long tbb = links[1][u][j].tb;
                        long w_start_time, w_end_time=end_time;
                        int busid = links[1][u][j].busid;
                        int father = u;
                        if(mark.exist(w)){
                            w_start_time = mark.m_data[w];
                        }else{
                            w_start_time = -1;
                        }
                        if(end_time-taa<w_end_time-w_start_time||end_time-taa==w_end_time-w_start_time
							&&(w_[w]==-1||w_[father]>=0&&w_[father]<w_[w]||leveltovertexid[father]<w_[w])){
                            mark.insert(w, taa);
                            nodes_pq[1].insert(w, end_time - taa);
                            if(busmark.m_data[father] == busid)busmark.insert(w, busid);
                            else busmark.insert(w,-1);
                            fathermark.insert(w,father);
//							if(end_time-taa==w_end_time-w_start_time)printf("%d",w_[w]);
							w_[w]=w_[father];
							if(w_[w]<0||leveltovertexid[father]<w_[w])w_[w]=leveltovertexid[father];
//							if(end_time-taa==w_end_time-w_start_time)printf(" %d\n",w_[w]);
                        }
                    }
                }
                for( int counter = 0; counter < mark.occur.m_num; counter++){
                    if(mark.exist(mark.occur[counter])){
                        int j = mark.occur[counter];
                        if(j!=v){
                            long ta = mark.m_data[j];
                            long tb = end_time;
                            int father = fathermark.m_data[j];
                            int busid = busmark.m_data[j];
                            assert(father>=0);
                            LabelQunituple<int, long, long,int, int, int> new_p(leveltovertexid[v],ta, tb, leveltovertexid[father], w_[j], busid);
                            add_Label(label[0][leveltovertexid[j]], new_p);
//							printf("%d out: %d %d %d %d %d\n",j,v,ta,tb,father,busid);
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
        children_pointer_linking();
        clock_t end_index_construction = clock();
        indexing_time = (double)(end_index_construction-start_index_construction)/CLOCKS_PER_SEC ;
        printf( "Index Construction Time: %0.2lf\n", indexing_time );
        cout<<"Total preprocessing time:"<< (ordering_time+indexing_time)<<endl;
        cout<<(lcnt)<<endl;
        cout<< (lcnt*1.0/N) <<endl;
    }

    void load_query_set(string query_file_name){
        FILE* query_file =fopen(query_file_name.c_str(), "r");
        int u,v;
        long ta, tb;
        while( fscanf(query_file, "%d %d %ld %ld\n", &u, &v, &ta, &tb) == 4){
            EdgeQuintuple<int, int, long, long, int>  eq(u,v,ta,tb, -1);
            query_sets.push_back(eq);
        }
        cout<<"Num of querys:"<<query_sets.size()<<endl;
        fclose(query_file);
    }

    void load_order_file(string file_name){
        cout<<file_name<<endl;
        FILE* file = fopen(file_name.c_str(), "r");
        int idx=0;
        int v =0;
		for ( ; fscanf(file,"%d",&v) == 1 ; ++idx){
            vertexid2level[idx] = v;
        }
        fclose(file);
    }

	LabelQunituple<int,long,long,int,int,int>*get_address(int v1,int v2,long ta,long tb)
	{
		int d,v,u,l,r,m,i;
		if(v1<v2)
		{
			d=1;
			u=v1;
			v=v2;
		}
		else
		{
			d=0;
			v=v1;
			u=v2;
		}
		l=-1;
		r=label[d][v].m_num;
		while(l+1<r)
		{
			m=(l+r)/2;
			if(label[d][v][m][0].v<u)l=m;
			else r=m;
		}
		if(label[d][v][r][0].v!=u)
		{
			return NULL;
		}
		l=-1;
		u=r;
		r=label[d][v][u].m_num;
		while(l+1<r)
		{
			m=(l+r)/2;
			if(ta>=0&&label[d][v][u][m].ta<ta||tb>=0&&label[d][v][u][m].tb<tb)l=m;
			else r=m;
		}
		if(r>=label[d][v][u].m_num||ta>=0&&label[d][v][u][r].ta!=ta||tb>=0&&label[d][v][u][r].tb!=tb)
		{
			return NULL;
		}
		return &label[d][v][u][r];
	}

    void children_pointer_linking(){
        for(int i=0; i<N; i++){
            for(int j=0; j<label[0][i].m_num; j++){
                if(label[0][i][j][0].v==i)continue;
                for( int k=0; k<label[0][i][j].m_num; k++){
                    int w=label[0][i][j][k].w;
                    if(w>=0)
                    {
                        label[0][i][j][k].iw=get_address(i,w,label[0][i][j][k].ta,-1);
                        label[0][i][j][k].wv=get_address(w,label[0][i][j][k].v,-1,label[0][i][j][k].tb);
                    }
                }
            }
            for(int j=0; j<label[1][i].m_num; j++){
                if(label[1][i][j][0].v==i)continue;
                for( int k=0; k<label[1][i][j].m_num; k++){
                    int w=label[1][i][j][k].w;
                    if(w>=0)
                    {
                    	//label[1][i][j][k]表示，顶点i能到达的第j个顶点的第k个标签
                        ///label[1][i][j][k].iw=get_address(w,i,-1,label[1][i][j][k].tb);
                        ///label[1][i][j][k].wv=get_address(label[1][i][j][k].v,w,label[1][i][j][k].ta,-1);
                    	label[1][i][j][k].iw=get_address(label[1][i][j][k].v,w,label[1][i][j][k].ta,-1);
                    	label[1][i][j][k].wv=get_address(w,i,-1,label[1][i][j][k].tb);
                    }
                }
            }
        }
    }


	void output(string index_file_str){
        cout<<"output index files"<<endl;
        string order_file_str=index_file_str+".order";
        index_file_str+=".index";
        cout<<"Index file: "<<index_file_str<<endl;
        cout<<"Order file: "<<order_file_str<<endl;
        FILE* index_file = fopen(index_file_str.c_str(), "w");
        string num=index_file_str+".num";
        FILE* index_num = fopen(num.c_str(), "w");
        //FILE* order_file = fopen(order_file_str.c_str(), "w");
        //fprintf(index_file, "%d\n", hybrid_order_start);
//        for(int i=0; i<N; i++){
//            fprintf(order_file, "%d\n", vertexid2level[i]);
//        }
        long num_labels=0;
		for(int i=0; i<N; i++){
			fprintf(index_file, "%d:", i);
			for(int j=0; j<label[0][i].m_num; j++)
			{
				num_labels+=label[0][i][j].m_num;
				for( int k=0; k<label[0][i][j].m_num; k++)
					fprintf(index_file, "%d %ld %ld %d %d ", label[0][i][j][k].v, label[0][i][j][k].ta, label[0][i][j][k].tb-label[0][i][j][k].ta, label[0][i][j][k].father, label[0][i][j][k].busid);
			}
			fprintf(index_file, "lin: ");
			for(int j=0; j<label[1][i].m_num; j++)
			{
				num_labels+=label[1][i][j].m_num;
				for( int k=0; k<label[1][i][j].m_num; k++)
					fprintf(index_file, "%d %ld %ld %d %d ", label[1][i][j][k].v, label[1][i][j][k].ta, label[1][i][j][k].tb - label[1][i][j][k].ta, label[1][i][j][k].father, label[1][i][j][k].busid);
			}
			fprintf(index_file, "-2\n");
			fprintf(index_num, "%d:%ld\n",i,num_labels);
			num_labels=0;
		}
        fclose(index_file);
        fclose(index_num);
    }

	void add_q(int &ql,int v,int f,int t)
	    {
	        int i,j,k;
	        q[ql]=v;
	        fq[ql]=f;
	        tq[ql]=t;
	        i=ql++;
	        while(i)
	        {
	            j=(i-1)/2;
	            if(tq[i]<tq[j])
	            {
	                k=q[i];
	                q[i]=q[j];
	                q[j]=k;
	                k=fq[i];
	                fq[i]=fq[j];
	                fq[j]=k;
	                k=tq[i];
	                tq[i]=tq[j];
	                tq[j]=k;
	            }
	            else break;
	            i=j;
	        }
	    }

	    void del_q(int &ql)
	    {
	        int i,j,k;
	        ql--;
	        q[0]=q[ql];
	        fq[0]=fq[ql];
	        tq[0]=tq[ql];
	        i=0;
	        while(1)
	        {
	            j=i*2+1;
	            if(j>=ql)break;
	            if(j+1<ql&&tq[j+1]<tq[j])j++;
	            if(tq[j]<tq[i])
	            {
	                k=q[i];
	                q[i]=q[j];
	                q[j]=k;
	                k=fq[i];
	                fq[i]=fq[j];
	                fq[j]=k;
	                k=tq[i];
	                tq[i]=tq[j];
	                tq[j]=k;
	            }
	            else break;
	            i=j;
	        }
	    }

	    int add_cvt(int u,int v,int t)
	        {
	            int s=1;
	            for(int i=get_ch(u,v,t);chv[i]>=0;i++)
	                s+=add_cvt(chv[i],v,t);
	            add_cv(u,v,s);
	            return s;
	        }
	    void add_cv(int u,int v,int s)
	        {
	            int i;
	            for(i=0;i<cv[u].size();i++)
	                if(cv[u][i].first==v)
	                {
	                    cv[u][i].second+=s;
	                    return;
	                }
	            cv[u].push_back(make_pair(v,s));
	        }

	    int get_ch(int u,int v,int t)
	        {
	            int r;
	            r=hash_fun(v,t)%tq[u];
	            while(1)
	            {
	                if(ch[u][r].w<0)return 0;
	                if(ch[u][r].v==v&&ch[u][r].t==t)return ch[u][r].w;
	                r++;
	                if(r==tq[u])r=0;
	            }
	        }
	void compute_index_order_coverage_heuristic(string fn){
        int ql,i,j,k,m,u,v,w,tu,tv,fu,s;
		char *starting=new char[N];
        tri fi;
		memset(starting,0,N);
        srand(time(NULL));
		FILE* rf = fopen(fn.c_str(),"r");
		if(rf==NULL)
		{
			//printf("file \"%s\" not found, all nodes will be sampled.\n",fn.c_str());
			memset(starting,1,N);
		}
		else
		{
            int choose_vertices=0;
			while(fscanf(rf,"%d",&i)>0)
                choose_vertices++;
            vector<int> shuffled_vs;
            for(int i=0; i<N; i++){
                shuffled_vs.push_back(i);
            }
            random_shuffle(shuffled_vs.begin(), shuffled_vs.end());
            for( int i=0; i<N; i++){
				starting[shuffled_vs[i]]=1;
            }
			fclose(rf);
		}
        clock_t start_time, end_time;
        start_time = clock();
        //puts("constructing trees...");
        m=0;
        for(i=0;i<links[0].m_num;i++)
            m+=links[0][i].m_num;
        s=m*4;
        fa.resize(N);
        ch.resize(N);
        p=new int[N+1];
        for(i=0;i<N;i++)
        {
            p[i]=i;
            fa[i].clear();
            ch[i].clear();
        }
        for(i=N-1;i;i--)
        {
            j= rand()%(i+1);//(unsigned int((rand()<<15)+rand()))%(i+1);
            k=p[i];
            p[i]=p[j];
            p[j]=k;
        }
        tb=new int[N];
        q=new int[m];
        fq=new int[m];
        tq=new int[m];
        memset(tb,-1,sizeof(int)*N);
		m=0;
        for(i=0;i<N;i++)
        {
            if(s<1)break;
            v=p[i];
			if(!starting[v])continue;
            //printf("%d nodes sampled\t\t%d space remainning...%c",m++,s,13);
            vector<int>tbs;
            tbs.clear();
            tbs.push_back(v);
			map<int,int>tta;
			tta.clear();
			int r=stamps;
			for(j=links[0][v].m_num-1;j>=0;j--)
				if(rand()%(j+1)<r)
				{
					tta[links[0][v][j].ta]=1;
					r--;
				}
            for(j=links[0][v].m_num-1;j>=0;)
            {
                tv=links[0][v][j].ta;
				if(stamps>0&&tta.find(tv)==tta.end())
				{
					j--;
					continue;
				}
				//printf("sampled %d %d %c",i,tv,13);
                tb[v]=tv;
                fi.v=v;
                fi.t=tv;
                fi.w=v;
                fa[v].push_back(fi);
                ql=0;
                while(j>=0&&links[0][v][j].ta==tv)
                {
                    add_q(ql,links[0][v][j].v,v,links[0][v][j].tb);
                   // if(ql==1)
                    //	cout<<ql<<endl;
                    j--;
                }
                while(ql)
                {
                    u=q[0];
                    fu=fq[0];
                    tu=tq[0];
                    del_q(ql);
                    if(tb[u]>=0&&tu>=tb[u])continue;
                    if(tb[u]<0)tbs.push_back(u);
                    tb[u]=tu;
                    fi.w=fu;
                    fa[u].push_back(fi);
                    fi.w=u;
                    ch[fu].push_back(fi);
                    s--;
                    for(k=0;k<links[0][u].m_num;k++)
                    {
                        w=links[0][u][k].v;
                        if(links[0][u][k].ta>=tu&&(tb[w]<0||links[0][u][k].tb<tb[w]))add_q(ql,w,u,links[0][u][k].tb);
                    }
                }
            }
            for(j=0;j<tbs.size();j++)
                tb[tbs[j]]=-1;
        }
        p[i]=-1;
        //puts("hashing the tree edges...");
        chv.clear();
        int as=0;
        for(i=0;i<N;i++)
        {
            //printf("%d %c",i,13);
            vector<tri>temp=fa[i];
            q[i]=temp.size();
            for(fq[i]=1;fq[i]*2/3<q[i];fq[i]=fq[i]*2+1);
            fa[i].resize(fq[i]);
            for(j=0;j<fq[i];j++)
                fa[i][j].w=-1;
            for(j=0;j<temp.size();j++)
            {
                for(k=hash_fun(temp[j].v,temp[j].t)%fq[i];fa[i][k].w>=0;k=((k>fq[i]-2)?0:k+1),as++);
                fa[i][k]=temp[j];
            }
            temp=ch[i];
            ch[i].clear();
            v=-1;
            for(j=0;j<temp.size();j++)
            {
                if(temp[j].v!=v||temp[j].t!=tv)
                {
                    chv.push_back(-1);
                    ch[i].push_back(temp[j]);
                    ch[i][ch[i].size()-1].w=chv.size();
                    v=temp[j].v;
                    tv=temp[j].t;
                }
                chv.push_back(temp[j].w);
            }
            temp=ch[i];
            for(tq[i]=1;tq[i]*2/3<temp.size();tq[i]=tq[i]*2+1);
            ch[i].resize(tq[i]);
            for(j=0;j<tq[i];j++)
                ch[i][j].w=-1;
            for(j=0;j<temp.size();j++)
            {
                for(k=hash_fun(temp[j].v,temp[j].t)%tq[i];ch[i][k].w>=0;k=((k>tq[i]-2)?0:k+1),as++);
                ch[i][k]=temp[j];
            }
        }
        chv.push_back(-1);
        //puts("calculating coverage...");
        cv.resize(N);
        for(i=0;i<N;i++)
            cv[i].clear();
        for(i=0;p[i]>=0;i++)
        {
            //printf("%d %c",i,13);
            v=p[i];
			if(!starting[v])continue;
            for(j=links[0][v].m_num-1;j>=0;)
            {
                for(tv=links[0][v][j].ta;j>=0&&links[0][v][j].ta==tv;j--);
                add_cvt(v,v,tv);
            }
        }
        //puts("ordering...");
        memset(p,0,sizeof(int)*N);
        for(i=0;i<N;i++)
        {
            //printf("%d %c",i,13);
            double max=-1;
            for(j=0;j<N;j++)
                if(!p[j])
                {
                    double cvs=(in_deg[j]+1)*(out_deg[j]+1)/1e9;
                    for(k=0;k<cv[j].size();k++)
                        cvs+=cv[j][k].second;
                    if(cvs>max)
                    {
                        max=cvs;
                        u=j;
                    }
                }
            vertexid2level[i]=u;
            p[u]=1;
            while(fq[u]>1)
            {
                while(1)
                {
                    j = rand()%fq[u];
                    //j=(unsigned int((rand()<<15)+rand()))%fq[u];
                    v=fa[u][j].v;
                    tv=fa[u][j].t;
                    w=fa[u][j].w;
                    if(w>=0)break;
                }
                j=del_tr(u,v,tv);
                if(w==u)continue;
                k=u;
                while(w!=k)
                {
                    add_cv(w,v,-j);
                    k=w;
                    w=get_fa(k,v,tv);
                }
            }
        }
        end_time = clock();
        ordering_time = (double)(end_time-start_time)/CLOCKS_PER_SEC ;
        cout<<"Ordering time: "<<ordering_time<<endl;
        //printf( "Ordering time: %0.2lf\n",(double)(end_time-start_time)/CLOCKS_PER_SEC );
        delete []in_deg;
        delete []out_deg;
        delete []p;
        delete []tb;
        delete []q;
        delete []fq;
        delete []tq;
    }

	 int del_tr(int u,int v,int t)
	    {
	        int s=1;
	        for(int i=get_ch(u,v,t);chv[i]>=0;i++)
	            if(!p[chv[i]])s+=del_tr(chv[i],v,t);
	        add_cv(u,v,-s);
	        del_fa(u,v,t);
	        return s;
	    }
	 int get_fa(int u,int v,int t)
	    {
	        int r;
	        r=hash_fun(v,t)%fq[u];
	        while(1)
	        {
	            if(fa[u][r].w==-1)
	            {
	                printf("get father (%d,%d,%d) failed\n",u,v,t);
	                return -1;
	            }
	            if(fa[u][r].v==v&&fa[u][r].t==t)
	            {
	                if(fa[u][r].w<0)printf("get father (%d,%d,%d) failed\n",u,v,t);
	                return fa[u][r].w;
	            }
	            r++;
	            if(r==fa[u].size())r=0;
	        }
	    }
	 void del_fa(int u,int v,int t)
	     {
	         int r;
	         r=hash_fun(v,t)%fq[u];
	         while(1)
	         {
	             if(fa[u][r].w==-1)
	             {
	                 printf("delete father (%d,%d,%d) failed\n",u,v,t);
	                 return;
	             }
	             if(fa[u][r].v==v&&fa[u][r].t==t)
	             {
	                 if(fa[u][r].w<0)printf("delete father (%d,%d,%d) failed\n",u,v,t);
	                 fa[u][r].w=-2;
	                 q[u]--;
	                 if(q[u]*3<fq[u])
	                 {
	                     vector<tri>temp=fa[u];
	                     fq[u]/=2;
	                     fa[u].resize(fq[u]);
	                     for(r=0;r<fq[u];r++)
	                         fa[u][r].w=-1;
	                     for(r=0;r<temp.size();r++)
	                         if(temp[r].w>=0)
	                         {
	                             int k;
	                             for(k=hash_fun(temp[r].v,temp[r].t)%fq[u];fa[u][k].w>=0;k=((k>fq[u]-2)?0:k+1));
	                             fa[u][k]=temp[r];
	                         }
	                 }
	                 return;
	             }
	             r++;
	             if(r==fq[u])r=0;
	         }
	     }

	 void output_order_file(string file_name){


		string order_file_name=file_name+".order";
		cout<<"output order files";
        FILE* order_file = fopen(order_file_name.c_str(), "w");
        for(int i=0; i<N; i++){
            fprintf(order_file, "%d\n", vertexid2level[i]);
        }
        fclose(order_file);
	}

};


int main(int argc, char** argv){
	HHL_temporal hhl_temporal;
	string GraphFileName="",IndexFileStem="",OrderFileName="",RootFileName="",QueryFileName = "";
	vector<ttr> result;
	hhl_temporal.init=1;
    hhl_temporal.load_temporal_graph("D:\\myttl\\austin.txt");
	//hhl_temporal.load_temporal_graph2("D:\\gz\\gz.txt");
    //hhl_temporal.load_temporal_graph("D:\\myttl\\example.txt");
	vector<ttr>r;
//    if( generate_query_or_not ){
//        hhl_temporal.generate_queries(GraphFileName, num_of_queries);
//        return 0;
//    }
//    if(output_index_or_not && IndexFileStem == ""){
//        cout<<"Missing IndexFileStem"<<endl;
//        return 0;
//    }

    //if( style == 1){
        //hhl_temporal.compute_index_order_coverage_heuristic(RootFileName);
   // }else if( style == 2){
	hhl_temporal.load_order_file("D:\\myttl\\heuristic-austin.index.order");
    //hhl_temporal.load_order_file("D:\\myttl\\heuristic-austin.index.order");
	//hhl_temporal.compute_index_order_coverage_heuristic("");
	//hhl_temporal.output_order_file("D:\\myttl\\austin.txt");
	    //hhl_temporal.load_order_file("D:\\myttl\\example.order");
    //}else{
    //    cout<<"Wrong order method!!!!"<<endl;
    //}

    hhl_temporal.compute_index_with_order_BFS();

    //if(output_index_or_not)
   //hhl_temporal.output("D:\\myttl\\austinC++20170404");
   // hhl_temporal.output("D:\\myttl\\exampleC++");
    clock_t start_query, end_query;
  //  if(do_query_test){
   hhl_temporal.load_query_set("D:\\myttl\\query-austin.txt");
        //hhl_temporal.load_query_set("D:\\myttl\\query-example.txt");

        printf( "Begin the Earliest Arrival Queries.\n" );
        start_query = clock();
       //pair<long,long> res=hhl_temporal.earliest_arrival_query(1,2,7,99);
       //cout<<res.first<<" to "<<res.second;
        int i,j;
//        for(i=0; i<hhl_temporal.query_sets.size(); i++){
//        	EdgeQuintuple<int, int, long, long, int> &eq = hhl_temporal.query_sets[i];
//            result = hhl_temporal.earliest_arrival_path_query(eq.u, eq.v, eq.ta, MAX_TIMESTAMP);
//            cout<<"query:"<<eq.u+1<<" to "<<eq.v+1<<" start-time "<<eq.ta<<endl;
//            for(j=0;j<result.size();j++){
//            	cout<<result[j].u+1<<"→"<<result[j].v+1<<" start from: "<<result[j].depar_time<<" take bus: "<<result[j].busid<<" arrive at: "<<result[j].arrival_time<<endl;
//            }
//            if(j==0)
//            	cout<<"path not found!";
//            cout<<endl;
//        }


                for(i=0; i<hhl_temporal.query_sets.size(); i++){
                	EdgeQuintuple<int, int, long, long, int> &eq = hhl_temporal.query_sets[i];
                    result = hhl_temporal.earliest_arrival_path_query(eq.u, eq.v, eq.ta, eq.tb);
                    cout<<"query:"<<eq.u+1<<" to "<<eq.v+1<<" start-time "<<eq.ta<<endl;
                    for(j=0;j<result.size();j++){
                    	cout<<result[j].u+1<<"→"<<result[j].v+1<<" start from: "<<result[j].depar_time<<" take bus: "<<result[j].busid<<" arrive at: "<<result[j].arrival_time<<endl;
                    }
                    if(j==0)
                    	cout<<"path not found!";
                    cout<<endl;
                }




       end_query = clock();
       printf( "Earliest Arrival Queries total query time: %0.2lf\n",(double)(end_query-start_query)/CLOCKS_PER_SEC );


//        for(int i=0; i<hhl_temporal.query_sets.size(); i++){
//            EdgeQuintuple<int, int, long, long, int> &eq = hhl_temporal.query_sets[i];
//            hhl_temporal.latest_departure_query( eq.u, eq.v, -(MAX_TIMESTAMP-1), eq.tb );
//        }

//        for(int i=0; i<hhl_temporal.query_sets.size(); i++){
//            EdgeQuintuple<int, int, long, long, int> &eq = hhl_temporal.query_sets[i];
//            hhl_temporal.fastest_duration_query( eq.u, eq.v, eq.ta, eq.tb);
//        }

    //}

}

