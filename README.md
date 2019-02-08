# csvToIMDG

Command Line Interface to read data from Comma Seperated Value file, identify an index as provided by the user, insert into an In Memory Data Grid.
<br />
Usage: csvToIMDG [-hvV] [-if=<indexField>] [-it=<indexType>] <file><br />
Imports csv files into IMDG Maps, assuming first field is index<br />
      <file>      The File name of source<br />
  -h, --help      Show this help message and exit.<br />
      -if, --indexField=<indexField><br />
                  The field to use an index<br />
      -it, --indexType=<indexType><br />
                  The java type for the index Field<br />
  -v, --verbose   Be verbose.<br />
  -V, --version   Print version information and exit.<br />
<br />            
