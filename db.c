#include "db.h"
#include<stdio.h>
#include"lock_reg.h"
#include<string.h>
#include<sys/uio.h>
#include<stdlib.h>
#include<unistd.h>
/* 
 *  the data concludes two file:idx file and data file 
 *  idx file concludes hash table,index record
 */

/*  
    |       hash table              |      index record                  |
    |_______________________________|____________________________________|
	|		|		 |			|	|							         |		
	|freeoff|chainoff|   .....	|\n	|								     |       index file.idx
	|_______|________|__________|___|____________________________________|
                |                  /                                     \
first hash   ___|                 /  |  composition of a piece of idx  |  \
value offset                     /___|_________________________________|___\
                                |    |chain|idx|     |   |data|   |data|    |        
                                |... |ptr  |len| key |sep|off |seq|len |... |        
                                |____|_____|___|_____|___|____|___|____|____|
                                         |                 |
       next offset of idx record      /__|     ____________| 
       on hash table                  \        |     
                                               |  data     |
                                         _____\|__record___|____________
                                        |      |           |   |        |
                                        |      |  datbuf   |\n |        |       data file.dat
                                        |______|___________|___|________|
                                               |               |  
                                               |  datalen      |
*/

#define  read_lock(fd,offset,whence,len) \
			lock_reg(fd,F_SETLK,F_RDLCK,offset,whence,len)
#define  readw_lock(fd,offset,whence,len)\
			lock_reg(fd,F_SETLKW,F_RDLCK,offset,whence,len)

#define  write_lock(fd,offset,whence,len)\
			lock_reg(fd,F_SETLK,F_WRLCK,offset,whence,len)
#define  writew_lock(fd, offset, whence, len) \
			lock_reg(fd,F_SETLKW,F_WRLCK,offset,whence,len)
#define  un_lock(fd,offset,whence,len)   \
			lock_reg(fd,F_SETLK,F_UNLCK,offset,whence,len)


DB* 
db_open(const char *name,int oflag,int mode)
{
	DB*  db;
	int  i,len;
	char asciiptr[PTR_SZ + 1],hash[(NHASH_DEF + 1)*PTR_SZ + 2];//创建hash表
	
	struct stat statbuf;
	len = strlen(name);
	if((db = _db_alloc(len))==NULL)
		perror("_db_alloc error");
	db->oflag = oflag;
	
	strcpy(db->name,name);
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
	return(db);
} 

DB*
 _db_alloc(int namelen)
{
	DB *db;
	
	if((db=calloc(1,sizeof(DB)))==NULL)
		perror("alloc error");
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
db_fetch(DB*db,const char*key)//取出
{
	char *ptr;
	if(_db_find(db,key,0)<0)//没找到key值对应
	{
		ptr=NULL;
		db->cnt_fetcherr++;
	}else{
		ptr=_db_readdat(db);//读出key值所对应的数据值
		db->cnt_fetchok++;
	}
	if(un_lock(db->idxfd,db->chainoff,SEEK_SET,1)<0)
	{		
		perror("un_lock error");
		abort();
		exit(1);
	}
	return ptr;
}

int 
_db_find(DB* db,const char *key,int writelock)
{
	off_t offset,nextoffset;
	
	db->chainoff = (_db_hash(db,key)*PTR_SZ)+db->hashoff;//hash表上的偏移
	db->ptroff = db->chainoff;
	
	if(writelock)
	{
		if(writew_lock(db->idxfd,db->chainoff,SEEK_SET,1)<0)
			perror("writew_lock");
	}else{
		if(readw_lock(db->idxfd,db->chainoff,SEEK_SET,1)<0)	
			perror("readw_lock");
	}
	offset = _db_readptr(db,db->ptroff);//如上图所知，读出的是地址是在下一个索引的hash表处
	while(offset!=0)
	{
		nextoffset=_db_readidx(db,offset);//将此key的数据读出来，放到db->idxbuf中
		if(strcmp(db->idxbuf,key)==0)
			break;
		db->ptroff = offset;//当前偏移记录
		offset = nextoffset;
	}
	if(offset==0)
		return -1;
	return 0;
}

hash_t 
_db_hash(DB *db,const char *key)
{
	hash_t hval;
	const char *ptr;
	char c;
	int i;
	hval=0;
	for(ptr=key,i=1;c=*ptr++;i++)//只要ptr加到末尾c=0,则中间的条件就不满足,跳出循环，此方法巧妙	
		hval +=c*i;
	return(hval % db->nhash);
}

off_t  
_db_readptr(DB* db, off_t offset)//偏移？
{
	char  asciiptr[PTR_SZ + 1];
	lseek(db->idxfd,offset,SEEK_SET);
	read(db->idxfd,asciiptr,PTR_SZ);
	
	asciiptr[PTR_SZ] = 0;
	return(atol(asciiptr));
}

off_t 
_db_readidx(DB *db,off_t offset)//读索引
{
	int 	i;
	char 	*ptr1,*ptr2;
	char 	asciiptr[PTR_SZ+1],asciilen[IDXLEN_SZ+1];
	struct iovec iov[2];
	if((db->idxoff = lseek(db->idxfd,offset,offset == 0 ? SEEK_CUR:SEEK_SET))==-1)
			perror("lseek error");
	iov[0].iov_base = asciiptr;
	iov[0].iov_len  = PTR_SZ;
	iov[1].iov_base = asciilen;
	iov[1].iov_len  = IDXLEN_SZ;//最大能存储4位数长度	
	
	if((i=readv(db->idxfd,iov,2)) != PTR_SZ + IDXLEN_SZ)//输入字符和字符长度
	{
		if(i==0 && offset == 0)
			return -1;//eof
	}

	asciiptr[PTR_SZ]=0;
	db->ptrval = atol(asciiptr);//散列表上下一个索引偏移地址

	if((db->idxlen = atoi(asciilen))<IDXLEN_MIN||db->idxlen>IDXLEN_MAX)//将字符转换为整数
		perror("invalid length");
	read(db->idxfd,db->idxbuf,db->idxlen);
	if(db->idxbuf[db->idxlen-1] != '\n')
		perror("missing newline");
	db->idxbuf[db->idxlen-1] = 0;
	
	ptr1=strchr(db->idxbuf,SEP);//ptr1指向第一次出现：的字符串
	*ptr1++=0;
	
	ptr2=strchr(ptr1,SEP);//ptr2指向第二次的：
	*ptr2++=0;
	
	if(strchr(ptr2,SEP)!=NULL)
		perror("too many separators");
	
	if((db->datoff = atol(ptr1))<0)
		perror("starting offset<0");
	if((db->datlen = atol(ptr2))<0)
		perror("invalid length");
	
	return(db->ptrval);

}	

char *
_db_readdat(DB *db)
{
	if(lseek(db->datfd,db->datoff,SEEK_SET) == -1)
		perror("lseek error");
	if(read(db->datfd,db->datbuf,db->datlen) != db->datlen)
		perror("read error");
	if(db->datbuf[db->datlen - 1] !='\n')
		perror("missing newline");
	db->datbuf[db->datlen -1]=0;
	return db->datbuf;
}

int
db_delete(DB *db,const char *key)
{
	int rc;
	if(_db_find(db,key,1)==0)
	{
		rc = _db_dodelete(db);
		db->cnt_delok++;
	}else{
		rc = -1;
		db->cnt_delerr++;
	}
	if(un_lock(db->idxfd,db->chainoff,SEEK_SET,1)<0)
		perror("un_lock error");
	return rc;
}

int 
_db_dodelete(DB *db)
{
	int     i;
	char	*ptr;
	off_t   freeptr,saveptr;
	
	for(ptr = db->datbuf,i=0;i<db->datlen-1;i++)
		*ptr++ = ' ';
	*ptr = 0;
	ptr = db->idxbuf;
	while(*ptr)
		*ptr++ = ' ';
	if(writew_lock(db->idxfd,FREE_OFF,SEEK_SET,1)<0)
		perror("writew_lock error");

	_db_writedat(db,db->datbuf,db->datoff,SEEK_SET);

	freeptr = _db_readptr(db,FREE_OFF);//回到文件起点
	saveptr = db->ptrval;//散列表存储的下一个偏移地址
	
	_db_writeidx(db,db->idxbuf,db->idxoff,SEEK_SET,freeptr);//清空索引记录的下一条索引记录
	_db_writeptr(db,FREE_OFF,db->idxoff);//更新freeptr值

	_db_writeptr(db,db->ptroff,saveptr);//更新散列表下一个记录存储的值

	if(un_lock(db->idxfd,FREE_OFF,SEEK_SET,1)<0)
		perror("un_lock error");	

	return 0;
}

void
_db_writedat(DB *db,const char *data,off_t offset,int whence)
{
	struct iovec iov[2];
	char   newline = '\n';
	
	if(whence == SEEK_END)
    	if(writew_lock(db->datfd,0,SEEK_SET,0)<0)//加了写琐
			perror("writew_lock error");

	if((db->datoff = lseek(db->datfd,offset,whence)) == -1)
		perror("lseek error");
	db->datlen = strlen(data) + 1;

	iov[0].iov_base = (char*) data;
	iov[0].iov_len  = db->datlen - 1;
	iov[1].iov_base = &newline;
	iov[1].iov_len  = 1;
	
	writev(db->datfd,iov,2);

	if(whence == SEEK_END)
		if(un_lock(db->datfd,0,SEEK_SET,0)<0)	
			perror("un_lock errror");
}

void
_db_writeidx(DB *db, const char *key, off_t offset, int whence, off_t ptrval)
{
	struct iovec iov[2];
	char         asciiptrlen[PTR_SZ + IDXLEN_SZ +1];
	int          len;
	
	if((db->ptrval = ptrval)<0 || ptrval>PTR_MAX)
	{
		perror("invalid ptr");
		exit(1);
	}
	sprintf(db->idxbuf,"%s%c%d%c%d\n",key,SEP,db->datoff,SEP,db->datlen);

	if((len=strlen(db->idxbuf))<IDXLEN_MIN||len>IDXLEN_MAX)
	{
		perror("invalid length");
		abort();
		exit(1);
	}
	sprintf(asciiptrlen,"%*d%*d",PTR_SZ,ptrval,IDXLEN_SZ,len);
	
	if(whence == SEEK_END)
	{
		if(writew_lock(db->idxfd,((db->nhash+1)*PTR_SZ)+1,SEEK_SET,0)<0)//
		{
			perror("writew_lock error");
			abort();
			exit(1);
		}
	}
	if((db->idxoff = lseek(db->idxfd,offset,whence)) == -1)
	{		
		perror("lseek error");
		abort();
		exit(1);
	}
	
	iov[0].iov_base = asciiptrlen;
	iov[0].iov_len  = PTR_SZ + IDXLEN_SZ;
	iov[1].iov_base = db->idxbuf;
	iov[1].iov_len  = len;
	
	if(writev(db->idxfd,iov,2) != PTR_SZ+IDXLEN_SZ+len)
	{
		perror("writev error");
		abort();
		exit(1);
	}
	if(whence == SEEK_END)
	{
		if(un_lock(db->idxfd,((db->nhash+1)*PTR_SZ)+1,SEEK_SET,0)<0)
		{
			perror("un_lock error");
			abort();
			exit(1);			
		}
	}
}

void 
_db_writeptr(DB *db,off_t offset,off_t ptrval)
{
	char asciiptr[PTR_SZ + 1];
	if(ptrval<0||ptrval>PTR_MAX)
	{		
		perror("invalid ptrval");
		exit(1);
	}
	sprintf(asciiptr,"%*d",PTR_SZ,ptrval);//加*表示以PTR——SZ为空出单位，右对齐
	
	if(lseek(db->idxfd,offset,SEEK_SET)==-1)
	{		
		perror("lseek error to ptr field");
		abort();
		exit(1);			
	}
	if(write(db->idxfd,asciiptr,PTR_SZ)!=PTR_SZ)
	{
		perror("write error of ptr filed");
		abort();
		exit(1);	
	}
}

int 
db_store(DB *db,const char *key,const char *data,int flag)
{
	int rc,keylen,datlen;
	off_t ptrval;
	
	keylen = strlen(key);
	datlen = strlen(data)+1;//1字节为换行符
	if(datlen < DATLEN_MIN||datlen>DATLEN_MAX)
	{		
		perror("invalid data length");
		abort();
		exit(1);	
	}
	if(_db_find(db,key,1)<0)//未找到
	{
		if(flag & DB_REPLACE)
		{
			rc = -1;
			db->cnt_storerr++;
			goto doreturn;
		}
	
		ptrval = _db_readptr(db,db->chainoff);//回到空闲链表上去找

		if(_db_findfree(db,keylen,datlen)<0)
		{
		/*
			key值大小和数据大小不相等的情况下，重新加入相关内容
        */
			_db_writedat(db,data,0,SEEK_END);
			_db_writeidx(db,key,0,SEEK_END,ptrval);
			_db_writeptr(db,db->chainoff,db->idxoff);
			db->cnt_store1++;
		}else{
		/*
			将数据根据偏移地址处添加，key值大小和数据大小相等的情况下。
		*/
			_db_writedat(db,data,db->datoff,SEEK_SET);
			_db_writeidx(db,key,db->idxoff,SEEK_SET,ptrval);
			_db_writeptr(db,db->chainoff,db->idxoff);
			db->cnt_store2++;			
		}
	}else{
		/*
			key值有记录的情况下
		*/
		if(flag & DB_INSERT)
		{
		/*
			已有记录，无法插入
		*/
			rc=1;
			db->cnt_storerr++;
			goto doreturn;
		}
		if(datlen !=db->datlen)
		{
		/*
			数据长度不相等，先删除
		*/
			_db_dodelete(db);
			ptrval = _db_readptr(db,db->chainoff);
			_db_writedat(db,data,0,SEEK_END);
			_db_writeidx(db,key,0,SEEK_END,ptrval);
			_db_writeptr(db,db->chainoff,db->idxoff);
			db->cnt_store3++;
		}else{
			_db_writedat(db,data,db->datoff,SEEK_SET);
			db->cnt_store4++;
		}
	}
	rc=0;
doreturn:
	if(un_lock(db->idxfd,db->chainoff,SEEK_SET,1)<0)
	{		
		perror("un_lock error");
		abort();
		exit(1);	
	}
	return rc;
}

int 
_db_findfree(DB *db,int keylen,int datlen)
{
	int  rc;
	off_t  offset,nextoffset,saveoffset;
	
	if(writew_lock(db->idxfd,FREE_OFF,SEEK_SET,1)<0)
	{		
		perror("writew_lock error");
		abort();
		exit(1);	
	}
	saveoffset=FREE_OFF;//位置指向空闲链表
	offset=_db_readptr(db,saveoffset);//读出空闲链表中第一条偏移地址
	
	while(offset!=0)
	{
		nextoffset =_db_readidx(db,offset);
		if(strlen(db->idxbuf) == keylen && db->datlen==datlen)
			break;
		saveoffset=offset;
		offset=nextoffset;
	}
	if(offset == 0)
		rc = -1;//找不到匹配
	else
	{
		_db_writeptr(db,saveoffset,db->ptrval);
		rc = 0;
	}
	if(un_lock(db->idxfd,FREE_OFF,SEEK_SET,1)<0)
	{
		
		perror("un_lock error");
		abort();
		exit(1);	
	}
	return rc;
}

void 
db_rewind(DB *db)
{
	off_t offset;
	offset=(db->nhash + 1)*PTR_SZ;
	
	if((db->idxoff = lseek(db->idxfd,offset+1,SEEK_SET))==-1)//索引记录起点位置，散列表结束处
	{		
		perror("un_lock error");
		abort();
		exit(1);	
	}	

}

char *
db_nextrec(DB *db,char *key)
{
	char c,*ptr;
	
	if(readw_lock(db->idxfd,FREE_OFF,SEEK_SET,1)<0)
	{		
		perror("readw_lock error");
		abort();
		exit(1);	
	}
	do{
		if(_db_readidx(db,0)<0)
		{
		/*
			重头开始查找
		*/
			ptr=NULL;
			goto doreturn;
		}
		ptr=db->idxbuf;
		while((c=*ptr++) !=0 && c==' ');
	}while(c==0);
	
	if(key !=NULL)
		strcpy(key,db->idxbuf);
	ptr = _db_readdat(db);
	db->cnt_nextrec++;
doreturn:
	if(un_lock(db->idxfd,FREE_OFF,SEEK_SET,1)<0)
	{
		perror("un_lock error");
		abort();
		exit(1);	
		
	}
	return ptr;
}


