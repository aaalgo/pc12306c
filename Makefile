CXXFLAGS +=-std=c++11 -O3 -pthread -fopenmp -g  -Wno-pointer-arith
LDFLAGS += -pthread -fopenmp
LDLIBS += -lboost_timer -lboost_chrono -lboost_system -lboost_program_options -lrt

.PHONY:	all mpi clean

all:	pc12306c

mpi:	pc12306c-mpi

pc12306c-mpi:	pc12306-mpi.cpp
	mpicxx -DUSE_MPI=1 $(CXXFLAGS) $(LDFLAGS) $^ $(LDLIBS)

clean:
	rm *.o pc12306c pc12306c-mpi
