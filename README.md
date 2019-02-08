# csvToIMDG

Command Line Interface to read data from Comma Seperated Value file, identify an index as provided by the user, insert into an In Memory Data Grid.

Usage: csvToIMDG [-hvV] [-if=<indexField>] [-it=<indexType>] <file>
Imports csv files into IMDG Maps, assuming first field is index
      <file>      The File name of source
  -h, --help      Show this help message and exit.
      -if, --indexField=<indexField>
                  The field to use an index
      -it, --indexType=<indexType>
                  The java type for the index Field
  -v, --verbose   Be verbose.
  -V, --version   Print version information and exit.
