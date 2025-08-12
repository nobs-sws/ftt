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