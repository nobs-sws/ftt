# Fast Transformation Tool

This is a dbt (data build tool) clone written in Rust with the goals of:
- learn Rust by doing something useful
- learn dbt internals
- become a better software engineer
- minimize the use of AI to help solve problems, I want to force my brain to do hard things and learn

## MVP

After some iterations, the current MVP presented in this version of the code will simulate dbt's behavior of two commands: seed and run.
The file data_basic.csv is the seed's target. I want to emulate the following:
- run dbt seed: load the csv file to a Snowflake database
- the csv loaded has a table automatically created
- run dbt run: read a .sql file with the desired transformation and creates the table in Snowflake

For everything to run, you need some things (in the future this will be automatic, the goal is to just "download" ftt and run, just like dbt):
- a Snowflake instance is needed (I'm using a free one for testing)
- snowsql installed. All the csv loading will be done with snowsql
- in snowflake, create a database, a schema, and an internal stage. In the future the tool will automatically get the values, but everythign for now is hard-coded

## Progress

dbt seed = ftt load

ftt_load() function has its basic function complete

ftt_run() is being developed
