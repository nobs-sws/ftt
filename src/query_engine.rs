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

/*
phase 3 =======================================


    let filepath = "/home/pelegolas/dev/rust/ftt/src/data/data_basic.csv";
    let table_path = String::from("/home/pelegolas/dev/rust/ftt/src/tables_folder/");
    let transformed_table_path = "/home/pelegolas/dev/rust/ftt/src/tables_folder/";
    let json_table_path = table_path.clone() + &table_name + ".json";

    match v2_load_csv_data(filepath) {
        Ok(data) => {
            let mut data_headers: HashMap<String, ColumnDataType> = HashMap::new();
            let mut data_columns_indexes: HashMap<String, i32> = HashMap::new();
            let mut data_remaining_records: Vec<csv::StringRecord> = Vec::<csv::StringRecord>::new();
            let mut mapped_remaining_records: HashMap<i32, csv::StringRecord> = HashMap::new();
            data_headers.clone_from(&data.0);
            mapped_remaining_records.clone_from(&data.1);
            data_columns_indexes.clone_from(&data.2);
            data_remaining_records.clone_from(&data.3);

            
            let mut new_table: Table = Table::new(&table_name);

            // preciso botar um numero de identificacao para as colunas, não há outra maneira

            for (column_name, dtype) in data_headers {
                let col_index = data_columns_indexes.get(&column_name).unwrap();
                let new_column = Column {
                    index: *col_index,
                    name: column_name,
                    data_type: match &dtype {
                        ColumnDataType::Integer(_ii) => "int".to_string(),
                        ColumnDataType::Float(_ff) => "float".to_string(),
                        ColumnDataType::String(_s) => "string".to_string(),
                        ColumnDataType::Boolean(_bb) => "bool".to_string(),
                        _ => "NULL".to_string(),                  
                    },
                    data: match dtype {
                        ColumnDataType::Integer(_i) => ColumnData::Integer(Vec::<i64>::new()),
                        ColumnDataType::Float(_f) => ColumnData::Float(Vec::<f64>::new()),
                        ColumnDataType::String(_s) => ColumnData::String(Vec::<String>::new()),
                        ColumnDataType::Boolean(_b) => ColumnData::Boolean(Vec::<bool>::new()),
                        _ => ColumnData::String(Vec::<String>::new()),
                    }
                };
                new_table.cols.push(new_column);
            }

            // reorganizando a tabela
            new_table.cols.sort_by_key(|c| c.index);


            for record in data_remaining_records {
                for column in &mut new_table.cols {
                    if let Some(value_str) = record.get(column.index.try_into().unwrap()) {
                        let _ = column.data.push_data(value_str);
                    } else {
                        eprintln!("Warning: No value found for column '{}' at index {} in record {:?}", column.name, column.index, record);
                    }
                }
            }

            // criando a tabela pelo csv
            let csv_table_name = table_name.clone();
            create_table_json(&new_table, &csv_table_name, &table_path);

            // nesse ponto a tabela já está mapeada e podemos seguir com o fluxo de leitura do arquivo SQL

            if args.command.eq(&run_action) {
                // sabemos a ação. depois adiciono o error handling para os outros campos
                // leitura do conteúdo do arquivo sql mandado
                let sql_file_contents = std::fs::read_to_string(&args.path).expect("something wrong");

                // agora mando isso para o query parser identificar as colunas para transformação
                let model_columns_to_transform = query_engine::identify_sql_command_columns(sql_file_contents);

                // as colunas foram corretamente identificadas! exemplo sendo usado: ["id", "age", "name"]
                // agora eu jogo esse vetor para a função de transformação. Antes disso, os dados da tabela em que o cósigo SQL está executando precisa já estar mapeado. Senão
                // não há como eu me basear. Isso é um outro processo que terá de ser feito à parte no futuro. Por agora, vamos assumir que já temos as colunas e os dados
                // mapeados.

                let deserialized_table = read_table_from_file(json_table_path).unwrap();

                let transformed_table = transform_columns(model_columns_to_transform, data_columns_indexes, deserialized_table);
                
                // criando o json com a tabela transformada
                create_table_json(&transformed_table, &transformed_table.name, &transformed_table_path.to_string());
            } else {
                println!("enter a valid action");
            }




            // ================================= PHASE 2: SQL ENGINE ==================================================
            /*
            // getting the columns that will be transformed
            let sql_command = "SELECT id, age FROM data_basic;";
            let sql_command_columns = query_engine::identify_sql_command_columns(sql_command.to_string());
            //println!("{:?}", sql_command_columns);


            // TODO agora preciso verificar: por acaso a coluna tem alguma modificação? tipo um SUM ou AVG? para fins de testes, vamos fazer sem primeiro

            // pegar a tabela pelo arquivo json e fazer um DEserialize em struct rust. ideia para o futuro: manter essas structs em um buffer temporario
            let des_table = read_table_from_file(table_path).unwrap();

            // passar o array com as colunas do SELECT statement e realizar a transformação
            let transformed_table = transform_columns(sql_command_columns, data_columns_indexes, des_table);

            // creating the json with transformed table
            //create_table_json(&transformed_table, &transformed_table.name, transformed_table_path.to_string());
            */
        },
        Err(e) => eprintln!("Erro: {}", e),
    }

*/