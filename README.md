# Fast Transformation Tool

This is a dbt (data build tool) clone written in Rust for educational purposes.

## How can I accomplish this? What are the steps to take?

- It will be a CLI

## What to do for the MVP

- Read a CSV file DONE
- Convert the CSV file to struct DONE
- Created a struct to hold the table data (a vector for each column). For the future, this needs to be generated on the fly. In other words, how does dbt stores the table structures when reading the data? remember the {{source}} macro DONE!!
- Run the model
- The model: reads the csv file and applies the transformation
- The model creates a new file with the updated data

## Lessons learned

reading a csv file was harder than I thought, but I made it. Without AI, just old school programming and google searching. I think I kinda grasped the Rust concepts
of Returning a Result/Option type, error handling and data types.

Result type shows if something was ok or error, so if something returns Result and we want the value, we need to use .unwrap() to get it.

BREAKTHROUGH

to represent a generic table type that can be dynamically modified, created, and so on, I create a simple struct Table that contains one Vector of columns. This way, I can add/remove columns when transforming the data. Column should be another struct, so Table is a Vector of Columns(struct). This way I don't even need to create each vector for the data

from Rust By Example on If Let pattern matching:

    // The `if let` construct reads: "if `let` destructures `number` into
    // `Some(i)`, evaluate the block (`{}`).
    if let Some(i) = number {
        println!("Matched {:?}!", i);
    }

I am trying to create a generic type for the Column, based on the enum ColumnDataType when you read the CSV and the types get inferred. However this is not working now, as I need to better understand the generics. Meanwhile, I will create one separate struct for each data type: String, i64, f64, boolean. Solved this with the help of Gemini and a new enum, instead of generic types.

Now I need to get the full csv data, we are only getting the headers

csv data mapped to a vec of tuples with the correct data types inferred. now what?

now we build the table!

table structure is built ok, creating the Column vectors with the correct ColumnDataTypes vectors within created. Now the next step is to fill the vectors (aka column data) with the correct values from the tuples.

added column index for the tuples


instead of this tuple confusion, I simplified things using hashmaps for:
    - mapping column indexes
    - mapping record indexes

Gemini helped me with the last function needed to populate the values in a columnar format! Now we have the table structure represented correctly with columns


next steps:
    - save the table structure to a local JSON file DONE
    - start the query engine
    - start CLI to:
        - run a 'ftt run' command
        - this command reads the sql file
        - transforms the data
        - then we select from the table

next steps on the JSON output format:
    - modularize the structs. we don't need to write the table name every write, that's dumb. we need to separate the columns somehow

## PHASE 3: the basics of the tool

At this point in time I have some basic notion of the order of events that need to happen for the tool to work properly. For the MVP, we'll try to simulate as much as possible dbt when running 

dbt run -s model

1. We read the SQL model file, which comprises of the folowing steps:
    1.1 Get the columns that will be transformed (already done, improvements later)
    1.2 Check for the existence of macros
PAUSE, let's expand on the macros idea

dbt has sources and ref macros (using jinja templates): {{ ref('table_name')}} and {{ source('table_name')}}
I don't remember the difference now, but they translate to the table name when dbt runs.
Since we are not connecting to anything yet, we need to simulate a DW/DB connection. In the MVP, the idea was to read a csv table, load to a DB/DW, and transform the data, loading the results to said DW/DB. I'll skip the loading DB/DW part, the simulation is the JSON files. In short:
    - for the first run: reads csv and creates a json file
    - other runs: read json file and creates new json with transformed data. Table/data updates for teh csv is something for the future, but I need to keep that in mind.
So, I'll use {{ table('table_name')}} to simulate dbt's ref macro.

    1.3 as it find the macro, replace it with the corresponding table name in the query
    1.4 return the list of columns and table name to get data from


ftt load

for the above workflow to work, I need to connect the tool to a database/warehouse with the existing data, so i chose postgresql for my tests.
the idea is to read the csv, check if it is already in the db, create the table or not, and then apply the transformation
the 'ftt load' command will be the 'dbt seed' command
so:
    - ftt load
    - look for csv files
    - read the csv and creates a postgres CREATE IF NOT EXISTS statement. if the table exists, just print a message like "table already exists, nothing to do."
    - this ends the loading part. now we move to the main transforming logic
        - ftt run -s model
        - checks if table exists
        - do the magic
        - create transformed table

maybe a data dictionary, something like an information schema would be very helpful


ok, let's do this correctly. now, the MVP will do 2 things:
    - run 'ftt load' to load the data_basics.csv file into snowflake stage and create its table
    - run 'ftt run -s model to do its magic
    - all error handling will be done later/as going on

ftt load step:
    - create a PUT command via snowflake API to load the csv file into a stage
    - create a CREATE TABLE with the loaded csv file via snowflake API
    - execute the queries
    - data coreclty loaded

ftt run steps:
    - map the model CLI argument to the table name using the table() macro
    - apply the transformation
    - create the new query
    - execute the query via snowflake API

observations:
    - configurations, how to deal with them? snowflake account, password, database, schema. hard-coded for now
    - error handling as we go or later on
    - snowflake stage area is assumed to be created
    - csv schema known beforehand


ftt load DONE, what to improve:
    - too many hard-coded things
    - works only for one file, with one db and schema. removing the hard-coding things will fix this
    - for the future: infer schema via snowflake or the code's logic
    - modularize steps/things. this is valid for the whole project, but let's keep it as simple as possible in the beginning


