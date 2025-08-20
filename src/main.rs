use std::path::Path;
use std::{collections::HashMap, io::BufReader};
use std::fs::File;
use std::io::Write;
use serde::{Deserialize, Serialize};

mod query_engine;
mod interface;

use clap::Parser;
use serde_json::to_string;

#[derive(Parser)]
struct Cli {
    command: String,
    flag: String,
    // path to sql file
    path: std::path::PathBuf
}


// tipo de dado para a coluna
#[derive(Debug, Clone, Deserialize, Serialize)]
enum ColumnDataType {
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    Null
}

// struct para os vetores de dados na coluna
#[derive(Debug, Serialize, Deserialize, Clone)]
enum ColumnData {
    Integer(Vec<i64>),
    Float(Vec<f64>),
    String(Vec<String>),
    Boolean(Vec<bool>)
}

impl ColumnData {
    // A helper method to push data based on its type
    fn push_data(&mut self, value: &str) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            ColumnData::Integer(vec) => {
                vec.push(value.parse::<i64>()?);
            }
            ColumnData::Float(vec) => {
                vec.push(value.parse::<f64>()?);
            }
            ColumnData::String(vec) => {
                vec.push(value.to_string());
            }
            ColumnData::Boolean(vec) => {
                vec.push(value.parse::<bool>()?);
            }
        }
        Ok(())
    }
}


impl ToString for ColumnDataType {
    fn to_string(&self) -> String {
        match self {
            ColumnDataType::Integer(i) => i.to_string(),
            ColumnDataType::Float(f) => f.to_string(),
            ColumnDataType::String(s) => s.clone(),
            ColumnDataType::Boolean(b) => b.to_string(),
            ColumnDataType::Null => "NULL".to_string(),
        }
    }
}
// Table struct para conter o vetor de colunas
#[derive(Debug, Serialize, Deserialize)]
struct Table {
    name: String,
    cols: Vec<Column>
}

impl Table {
    fn new(name: String) -> Self {
        Table {
            name,
            cols: Vec::new()
        }
    }
}
// ----------------- column structs ------------------------
// Column struct
#[derive(Debug, Serialize, Deserialize, Clone)]
struct Column {
    index: i32,
    name: String,
    data_type: String,
    data: ColumnData,
}

/*
como é o uso da ferramenta?

ftt run --select model -> é o comando para executar a transformação que tem no modelo sql

CLI
- lê os argumentos: action (run), flag (--select), path_to_sql_model (model)
para fins de teste, iremos usar apenas o exemplo acima. Porém, melhor já ir preparando para os outros casos
- CLI manda para a engine o que deve ser feito
- engine lê o SQL do modelo e vê a transformação
- engine manda para o DATA_TRANSFORMER para gerar a nova tabela transformada
- um JSON com a nova tabela é gerado, dentro da pasta tables_transformed

*/

fn main() {
    let args = Cli::parse();
    // depois da CLI ler os argumentos, verificar primeiro a ação
    let run_action = String::from("run");



    let filepath = "/home/pelegolas/dev/rust/ftt/src/data/data_basic.csv";
    let table_path = "/home/pelegolas/dev/rust/ftt/src/tables_folder/output.json";
    let transformed_table_path = "/home/pelegolas/dev/rust/ftt/src/tables_folder/";

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

            
            //data_headers.clone_from(&data);
            let mut new_table: Table = Table::new("new_table".to_string());

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

                let deserialized_table = read_table_from_file(table_path).unwrap();

                let transformed_table = transform_columns(model_columns_to_transform, data_columns_indexes, deserialized_table);
                
                // criando o json com a tabela transformada
                create_table_json(&transformed_table, &transformed_table.name, transformed_table_path.to_string());
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

}


fn transform_columns(column_list: Vec<String>, columns_indexes: HashMap<String, i32>, table_to_transform: Table) -> Table {
    let mut transformed_table = Table::new("teste".to_string());
    let table_copy: Table = table_to_transform;

    // pegando os indices das colunas que estão no comando sql
    let mut col_indexes = Vec::new();
    for column_name in &column_list {
        let col_index = *columns_indexes.get(column_name).unwrap();
        col_indexes.push(col_index);
    }

    for index in col_indexes {
        let column_to_add = table_copy.cols[index as usize].clone();
        transformed_table.cols.push(column_to_add);
    }
    
    transformed_table.cols.sort_by_key(|c| c.index);

    transformed_table
}



// reads json table data to a table struct
fn read_table_from_file<P: AsRef<Path>>(path: P) -> Result<Table, Box<dyn std::error::Error>> {
    // Open the file in read-only mode with buffer.
    let file = File::open(path)?;
    let reader = BufReader::new(file);

    let t = serde_json::from_reader(reader)?;

    Ok(t)
}

// criar arquivo json com tabela
fn create_table_json(table_to_transform: &Table, table_name: &str, table_path: String) {
    let table_json = serde_json::to_string_pretty(&table_to_transform).unwrap();
    let path = table_path + "/" + table_name + ".json";
    // write to file
    match File::create_new(path) {
        Ok(mut file_created) => file_created.write_all(table_json.as_bytes()).unwrap(),
        Err(file_error) => eprintln!("error: {:?}", file_error)
    };

}


// retornar apenas o hashmap com as colunas e seus tipos
fn v2_load_csv_data(filename: &str) -> Result<(HashMap<String, ColumnDataType>, HashMap<i32, csv::StringRecord>, HashMap<String, i32>,Vec<csv::StringRecord> ),Box<dyn std::error::Error>> {
//Result<HashMap<String, ColumnDataType>, Box<dyn std::error::Error>> {
    let mut rdr = csv::Reader::from_path(filename).expect("No CSV file found!");

    // getting the csv headers and creating a column struct for each one
    let headers = rdr.headers()?.clone();

    // pegando o iterador dos registros
    let mut records_iter = rdr.records();
    let mut first_data_row_types: HashMap<String, ColumnDataType> = HashMap::new();
    let mut remaining_records: Vec<csv::StringRecord> = Vec::new();
    // para mapear cada registro em sua posição
    let mut mapped_remaining_records: HashMap<i32, csv::StringRecord> = HashMap::new();
    // para mapear os nomes das colunas com um indice
    let mut columns_indexes: HashMap<String, i32> = HashMap::new();

    if let Some(first_record_result) = records_iter.next() {
        let first_record: csv::StringRecord = first_record_result?;

        for (i, field) in first_record.iter().enumerate() {
            if let Some(header_name) = headers.get(i) {
                let inferred_type_for_column = infer_column_data_type(field);
                first_data_row_types.insert(header_name.to_string(), inferred_type_for_column);
                columns_indexes.insert(header_name.to_string(), i as i32);
            }
        }
        remaining_records.push(first_record);

    } else {
        return Ok((HashMap::new(), HashMap::new(), HashMap::new(), Vec::<csv::StringRecord>::new()));
    }

    for record_result in records_iter {
        let record: csv::StringRecord = record_result?;
        remaining_records.push(record);
    }

    // remaining_records possui os registros na ordem correta, agora é só colocar um indice para cada um
    for (i, record) in remaining_records.iter().enumerate() {
        mapped_remaining_records.insert(i as i32, record.clone());
    }

    Ok((first_data_row_types, mapped_remaining_records, columns_indexes, remaining_records))

}

// inferencia de tipo de dado da coluna
fn infer_column_data_type(row: &str) -> ColumnDataType {

    if row.is_empty() {
        return ColumnDataType::Null;
    }

    if let Ok(i) = row.parse::<i64>() {
        return ColumnDataType::Integer(i);
    }

    if let Ok(f) = row.parse::<f64>() {
        return ColumnDataType::Float(f);
    }

    if row.eq_ignore_ascii_case("true") {
        return ColumnDataType::Boolean(true);
    }

    if row.eq_ignore_ascii_case("false") {
        return ColumnDataType::Boolean(false);
    }

    ColumnDataType::String(row.to_string())
}







