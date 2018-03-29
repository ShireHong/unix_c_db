#ifndef  _DB_H
#define  _DB_H

#include<sys/types.h>
#include<sys/stat.h>
#include<fcntl.h>
#include<stddef.h>

#define DB_INSERT  1
#define DB_REPLACE 2

#define PTR_SZ    		6
#define PTR_MAX    999999
#define NHASH_DEF  	  137
#define FREE_OFF        0
#define HASH_OFF   PTR_SZ

#define IDXLEN_SZ       4
#define IDXLEN_MIN      6
#define IDXLEN_MAX   1024
#define SEP           ':'
#define DATLEN_MIN      2
#define DATLEN_MAX   1024
 
typedef struct {
	int idxfd;
	int datfd;
	int oflag;
	char  *idxbuf;
	char  *datbuf;
	char  *name;
	off_t idxoff;
	size_t idxlen;
	off_t datoff;
	size_t datlen;
	off_t ptrval;
	off_t ptroff;
	off_t chainoff;
	off_t hashoff;
	int   nhash;
	long  cnt_delok;
	long  cnt_delerr;
	long  cnt_fetchok;
	long  cnt_fetcherr;
	long  cnt_nextrec;
	long  cnt_store1;
	long  cnt_store2;
	long  cnt_store3;
	long  cnt_store4;
	long  cnt_storerr; 
}DB;

typedef unsigned long hash_t;

DB*     db_open(const char *,int,int);
DB*	   _db_alloc(int);
void   _db_rewind(DB*);
char*   db_fetch(DB *,const char *);
int    _db_find(DB *,const char *,int);
hash_t _db_hash(DB *,const char *);
off_t  _db_readptr(DB *,off_t );
off_t  _db_readidx(DB *,off_t );
char*  _db_readdat(DB*);
int     db_delete(DB *,const char *);
int    _db_dodelete(DB *);
void   _db_writedat(DB *,const char *,off_t, int );
void   _db_writeidx(DB *,const char *,off_t, int,off_t);
void   _db_writeptr(DB *, off_t, off_t);



#endif 	










