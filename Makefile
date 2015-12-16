CXXFLAGS +=-std=c++11 -O3 -pthread -fopenmp -g 
LDFLAGS += -pthread -fopenmp
LDLIBS += -lboost_timer -lboost_chrono -lboost_system -lboost_program_options 

all:	pc12306c

clean:
	rm *.o pc12306c
