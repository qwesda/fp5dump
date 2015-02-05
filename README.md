# Fp5Dump

Fp5Dump allows you to parse, read and export `.fp5` files to postgres `.psql` files.
 
Since FileMaker 5/6 stores all values as strings and does not enforce a particular 
format for dates/numbers all fields will be dumped as `TEXT` or `TEXT[]` for *repeated fields*.

Supported field types are `TEXT`, `NUMBER`, `DATE`, `TIME`, `CALC`, `SUMMARY`. Fields of type 
`CALC` or `SUMMARY` can only be dumped if they are configured to be stored in the file.
`CONATINER` fields are not (yet) supported. 


## Installation

- download or clone this repositiory
- open a terminal 
- change to the directory of the repository
- `python3 setup.py install`

## Basic usage

`fp5dump [general-options] {action} <file> [action-options]`

**general-options**

`-v` show info messages  
`-vv` show debug messages 

**action** is one of `list-fields` `count-records` `dump-blocks` `dump-records`

**file** a readable filemaker 5/6 file

 
### list-fields

lists the fields in a fp5 file

```
fp5dump list-fields database.fp5 
        [--encoding <encoding>] [--show-unstored] [--include-fields-types <type>]
```

**`--include-fields-types <type>`**  
`type` *one or more of* `TEXT` `NUMBER` `DATE` `TIME` `CALC` `SUMMARY` `GLOBAL` `CONTAINER`  
by default all field types are included

**`--show-unstored`**  
show fields (e.g. calculations, summaries and globals) who's values are not stored in the file

**`--encoding <encoding>`**  
*one of* `ascii` `cp1252` `latin_1` `macroman`  
the encoding to interpret strings values 

 
### count-records

shows the number of records in a fp5 file

`fp5dump count-records database.fp5`

 
### dump-blocks

dumps the data or index blocks of a fp5 file in their logical order

```
fp5dump dump-blocks database.fp5 
         [--output <filename>] [--type <data|index>] [--with-path <path>]
```

**`-o <filename>` `--output <filename>`**  
the name of the file the blocks are dumped to  
by default a file with the suffix `data`, `index` or `<path>.data` will be written in the same directory

**`--type`**  
one of `index` `data`, default is `data`
dump either the index blocks of the fp5 file or the data blocks

**`--with-path <path>`**  
only valid with `--type data` 
dumps only data block containing nodes of a certain path. paths are written as hex-bytes separated by `/`.  
e.g. `'03'`, `'03/02'`, `'05/7E46/42'`  

 
### dump-records

dump the records of fp5 file to a psql file

```
fp5dump dump-records database.fp5 
        [--output <filename>] [--encoding <encoding>] [--progress]
        [--include-fields <name>] [--include-fields-like <regex>] 
        [--ignore-fields <name>] [--ignore-fields-like <regex>] 
        [--ignore-field-types <type>]
```

**`-o <filename>` `--output <filename>`**  
the name of the file the blocks are dumped to
by default a file with the suffix `psql` will be written in the same directory

**`--encoding <encoding>`**  
`encoding` one of `ascii` `cp1252` `latin_1` `macroman`  
the encoding to interpret strings values 

**`-p` `--progress`**  
shows the progress and estimated time to completion

**`--include-fields <name>`** and **`--include-fields-like <regex>`**  
includes fields in the dump which are specified by `name` or which match one of the `regex`  
if neither are specified all fields will be dumped 

**`--ignore-fields <name>`** and **`--ignore-fields-like <regex>`**  
excludes fields in the dump which are specified by `name` or which match one of the `regex`  
exclusions overwrite inclusions

**`--ignore-field-types <type>`**  
`type` *one or more of* `TEXT` `NUMBER` `DATE` `TIME` `CALC` `SUMMARY` `GLOBAL` `CONTAINER`  
excludes fields of certain types  
exclusions overwrite inclusions
