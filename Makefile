MPICXX=mpicxx
CXXFLAGS +=-std=c++11 -O3 -pthread -fopenmp -g  -Wno-pointer-arith
LDFLAGS += -pthread -fopenmp
LDLIBS += -lboost_timer -lboost_chrono -lboost_system -lboost_program_options -lrt

.PHONY:	all mpi clean

all:	pc12306c

mpi:	pc12306c-mpi

pc12306c-mpi:	pc12306c-mpi.cpp
	$(MPICXX) -DWITH_MPI=1 -o $@ $(CXXFLAGS) $(LDFLAGS) $^ $(LDLIBS)

clean:
	rm *.o pc12306c pc12306c-mpi
