/*
    what does the parser/engine does?

    it reads the sql file
    interprets the command
    calls the function to transform data
*/

pub fn identify_sql_command_columns(sql_command: String) -> Vec<String> {
    let start_delimiter = "SELECT";
    let end_delimiter = "FROM";

    // cortar o comando sql em slices
    let sliced_sql_command: Vec<&str> = sql_command.split(',').collect();
    //println!("sliced_sql_command: {:?}", sliced_sql_command);
    let v = sql_command.replace(start_delimiter, "");

    // letter F from "FROM"
    let end_delimiter_first_byte = v.find(end_delimiter).unwrap();
    let v2 = &v[0..end_delimiter_first_byte];

    let final_string: Vec<&str> = v2.split(",").collect();
    let mut trimmed_final_string: Vec<String> = Vec::new();

    // trimming each column name
    for column in final_string {
        let trimmed_column = column.trim();
        trimmed_final_string.push(trimmed_column.to_string());
    }

    trimmed_final_string
}