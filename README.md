BenchTool for Fedora 3/4
=======================================================================================
This is a simple ingest benchmark to compare performance of Fedora 3 and Fedora 4. 
The suite consists of two main classes responsible for running the benchmarks. 


### Usage

```
usage: BenchTool
 -a,--action <action>             the action to perform.
                                  [ingest|read|update|delete]
 -f,--fedora-url <fedora-url>     the URL of the fedora instance
 -h,--help                        print the help screen
 -l,--log <log>                   the log file to which the durations will
                                  get written
 -n,--num-actions <num-actions>   the number of actions performed
 -p,--password <password>         the number of threads used for
                                  performing all actions
 -s,--size <size>                 the size of the individual binaries used
 -t,--num-threads <num-threads>   the number of threads used for
                                  performing all actions
 -u,--user <user>                 the number of threads used for
                                  performing all actions
```

Fedora 3
--------

##### Example
Login with the user `fedoraAdmin` and the passwd `changeme` and ingest 1000 Objects each with one datastream of 1024kb size 
```
#> java -jar target/bench-tool-${VERSION}-jar-with-dependencies.jar -f http://localhost:8080/fedora -u fedoraAdmin -p changeme -s 1048576 -n 1000 
```

Login with the user `fedoraAdmin` and the passwd `changeme` and update 1000 Objects each with one datastream of 1024kb size 

```
#> java -jar target/bench-tool-${VERSION}-jar-with-dependencies.jar -f http://localhost:8080/fedora -u fedoraAdmin -p changeme -s 1048576 -n 1000 -a update
```

Fedora 4
--------

#### Example
Ingest 1000 Objects each with one datastream of 1024kb size using a max of 15 threads 

```
#> java -jar target/bench-tool-${VERSION}-jar-with-dependencies.jar -f http://localhost:8080/fcrepo -s 1048576 -n 1000 -t 15 
```

Delete 1000 Objects with a single thread

```
#> java -jar target/bench-tool-${VERSION}-jar-with-dependencies.jar -f http://localhost:8080/fcrepo -s 1048576 -n 1000 -t 15 -a delete 
```

Results
-------
The durations file can be easily turned into a graph using gnuplot

#### Example
```
gnuplot> plot "ingest.log" title "FCRepo3 Ingest" with lines
```
