#include "db.h"
#include<stdio.h>
#include"lock_reg.h"
#include<string.h>

#define writew_lock(fd, offset, whence, len) lock_reg(fd,F_SETLKW,F_WRLCK,of    fset,whence,len)
#define un_lock(fd,offset,whence,len)    lock_reg(fd,F_SETLK,F_UNLCK,offset,    whence,len)


DB* 
db_open(const char *name,int oflag,int mode)
{
	DB*  db;
	int  i,len;
	char asciiptr[PTR_SZ + 1],hash[(NHASH_DEF + 1)*PTR_SZ + 2];
	
	struct stat statbuf;
	len = strlen(pathname);
	if((db = _db_alloc(len))==NULL)
		perror("_db_alloc error");
	db->oflag = oflag;
	
	strcpy(db->name,pathname);
	strcat(db->name,".idx");
	
	db->idxfd=open(db->name,oflag,mode);

	strcpy(db->name+len,".dat");
	db->datfd=open(db->name,oflag,mode);
	
	if((oflag & (O_CREAT|O_TRUNC)) == (O_CREAT|O_TRUNC))
	{
		writew_lock(db->idxfd,0,SEEK_SET,0);
		fstat(db->idxfd,&statbuf);
		if(statbuf.st_size==0)//如果该文件总大小为0
		{
			sprintf(asciiptr,"%*d",PTR_SZ,0);
			hash[0]=0;
			for(i=0;i<(NHASH_DEF + 1);i++)
				strcat(hash,asciiptr);
			strcat(hash,"\n");
			
			i=strlen(hash);
			write(db->idxfd,hash,i);
		}
		un_lock(db->idxfd,0,SEEK_SET,0);
	}	
	db->nhash = NHASH_DEF;
	db->hashoff = HASH_OFF;
	db_rewind(db);
	rreturn(db);
} 

static DB*
 _db_alloc(int namelen)
{
	DB *db;
	
	if((db=calloc(1,sizeof(DB)))==NULL)
		prror("alloc error");
	db->idxfd = db->datfd = -1;
	
	db->name=malloc(namelen+5);
	db->idxbuf=malloc(IDXLEN_MAX + 2);			
	db->datbuf=malloc(DATLEN_MAX + 2);

	return (db);
			
}

int 
_db_free(DB*db)
{
	if(db->idxfd >=0 && close(db->idxfd)<0)
		perror("index close error");
	if(db->datfd >=0 && close(db->datfd)<0)
		perror("data close error");
	db->idxfd = db->datfd = -1;
	
	if(db->idxbuf !=NULL)
		free(db->idxbuf);
	if(db->datbuf !=NULL)
		free(db->datbuf);
	if(db->name !=NULL)
		free(db->name);
	free(db);
	return 0; 	
}

void
db_close(DB *db)
{
	_db_free(db);
}


char *
db_fetch(DB*db,const char*key)
{
	char *ptr;
	if(_db_find(db,key,0)<0)
	{
		ptr=NULL;
		db->cnt_fetcherr++;
	}else{
		ptr=_db_readdat(db);
		db->cnt_fetchok++;
	}
	if(un_lock(db_idxfd,db->chainoff,SEEK_SET,1)<0)
		perror("un_lock error");
	return ptr;
}

int 
_db_find(DB* db,const char *key,int writelock)
{
	off_t offset,nextoffset;
	
	db->chainoff = (_db_hash(db,key)*PTR_SZ)+db->hashoff;
	db->ptroff = db->chainoff;
	
	if(writelock)
	{
		if(writew_lock(db->idxfd,db->chainoff,SEEK_SET,1)<0)
			perror("writew_lock");
	}else{
		if(readw_lock(db->idxfd,db->chainoff,SEEK_SET,1)<0)	
			perror("readw_lock");
	}
	offset = _db_readptr(db,db->ptroff);
	while(offset!=0)
	{
		nextoffset=_db_readidx(db,offset);
		if(strcmp(db->idxbuf,key)==0)
			break;
		db->ptroff = offset;
		offset = nextoffset;
	}
	if(offset==0)
		return -1;
	return 0;
}



hast_t 
_db_hash(DB *db,const char *key)
{
	hast_t hval;
	const char *ptr;
	char c;
	int i;
	hval=0;
	for(ptr=key,i=1;c=*ptr++;i++)//只要ptr加到末尾c=0,则中间的条件就不满足，                                   跳出循环，此方法巧妙	
		hval +=c*i;
	return(hval % db->nhash)
}


off_t  
_db_readptr(DB* db, off_t offset)
{
	char  asciiptr[PTR_SZ + 1];
	lseek(db->idxfd,offset,SEEK_SET);
	read(db->idxfd,asciiptr,PTR_SZ);
	
	asciiptr[PTR_SZ] = 0;
	return(atol(asciiptr));
}





