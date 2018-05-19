# SparkApplication

## Help

To launch the application use the GUI with a double-clic on the generated JAR

Or use the command line with these parameters :

`--type {coloring | hypergraph}` choose your type of algorithm

---

### If `type` is `coloring`

`--algo {1,2,3}`    choose your version
    
`--loops N`    number of loops run by spark on the same data
    
`--partitions N`    number of partitions for each RDD (0 = auto)
    
`--max_iterations N`    maximum number of iteration before quit
    
`--checkpoint_interval N`    interval between two checkpoints (0 = off)
    
`--input {file | generated}`    choose where your input data come from
    
#### If `input` is `file`
    
`--path "<path to the file>"`    path to your file
        
`--isGraphviz {true | false}`    is your file formatted with the graphviz format
        
#### If `input` is `generated`
    
`--n N`    number of variables
        
`--t N`    number of variables on each group
        
`--v N`    number of value for one variable

---

### If `type` is `hypergraph`

`--algo {1,2}`    choose your version
    
`--loops N`    number of loops run by spark on the same data
    
`--partitions N`    number of partitions for each RDD (0 = auto)
    
`--input {file | generated}`    choose where your input data come from
    
#### If `input` is `file`

`--path "<path to the file>"`    path to your file
       
#### If `input` is `generated`
    
`--n N`    number of variables
     
`--t N`    number of variables on each group
       
`--v N`    number of value for one variable

---

### Tips : 

Your can put all this parameters in one config file (each parameter on one line) and call it with `@"<path to the config file>"` [Example](../master/config_file_example.cfg)

To increase the memory available for spark put the java parameter `-Xmx` before any other parameter.
For exemple to use 4Go of RAM, type : `java -Xmx4G -jar <jar path> ...`
