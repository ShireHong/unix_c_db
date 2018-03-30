#include<stdio.h>
#include<stdlib.h>
#include<unistd.h>
#include "db.h"

#define FILE_MODE (S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)


int main()
{
	DB *db;
	db=db_open("db1",O_RDWR|O_CREAT|O_TRUNC,FILE_MODE);
    db_store(db,"xiaohu","student",DB_INSERT);
    db_store(db,"jianghao","student",DB_INSERT);
    db_store(db,"likelei","student",DB_INSERT);
	db_close(db);
	return 0;
}
