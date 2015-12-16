# pc12306c

Auther: Wei Dong wdong@wdong.org

This is a stress test client of https://github.com/weiqj/pc12306.

## Building

The code depends on a C++11 compiler and the Boost library.

## Command line parameters

```
  -h [ --help ]                    produce help message.
  -s [ --server ] arg (=localhost)
  -p [ --port ] arg (=12306)
  --trains arg (=5000)
  --segments arg (=10)
  --seats arg (=3000)
  --max-length arg (=5)
  -N arg (=10000000)               queries per client
  -C arg (=4)                      number parallel clients
  -B arg (=20)                     request batch size
  -Q arg (=1000)                   maximal outstanding request per client
  -S arg (=100)                    if queue is fall, sleep this # us
  --cycle arg (=1)                 print counters every cycle seconds
```


Explanation of some parameters:
- C: each client runs two threads, one for reading and one for writing,
  so total thread number is 2C.
- B: fit multiple requests into a single send request might increase
  throughput.
- Q: this is to preventing sending out too many requests before they
  can be processed by server.

## Performance Measures

- ooo.time.max: ooo == out of order.  Usually smaller respID should 
arrive before bigger respID.  This value is the maximal time a bigger
respID has arrived before a smaller respID.
- fill.rate: the server keeps the state across multiple invocation of
the client.  This value is meaningful only on the first invocation of
client after server restart, because subsequent invocations of client
have no means to track how many seats were sold before. This value of
the first N invocation of client can be added up for the total rate of
seats sold.

## Known Issues

There's some Heisenbug causing the program to crash when evaluation
conflict1 and/or conflict2.

