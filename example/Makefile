objects = main.o db.o lock_reg.o

edit : $(objects)
	cc -o edit $(objects)

main.o:db.h
db.o: lock_reg.h

.PHONY:clean
clean:
	rm edit $(objects)

