use std::path::Path;
use std::{collections::HashMap, io::BufReader};
use std::fs::File;
use std::io::Write;
use serde::{Deserialize, Serialize};
use std::process::Command;

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
#[derive(Debug, Serialize, Deserialize, Clone)]
struct Table {
    name: String,
    cols: Vec<Column>
}

impl Table {
    fn new(name: &String) -> Self {
        Table {
            name: name.to_string(),
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
    // removendo ".sql" do nome do modelo, para criar o json e a tabela com o nome do arquivo SQL
    let table_name = args.path.to_str().unwrap().replace(".sql", "").replace("models/", "");
    ftt_load(args.path);

    // lendo os contents do arquivo sql

    // depois de substituir o nome da tabela da macro, salvar a nova query em um lugar temporario
    // agora, em teoria, essa query deve ser jogada em um database e executada

    //acho que preciso fazer um information schema para que as queries sejam executadas corretamente

}

// cria a tabela no db a partir do csv
fn ftt_load(csv_file_path: std::path::PathBuf) {
    /*
        passos para carregar um csv, fazr inferencia de schema, criar uma tabela com os nomes das colunas, e copiar os dados do csv
        1 - ter um file format para o csv
        2 - ter um stage para receber arquivos
        3 - carregar o arquivo csv através do PUT
        4 - criar a tabela esqueleto usando CREATE USING TEMPLATE
        5 - fazer o COPY INTO com os dados
     */


    // caminho do arquivo que quero carregar
    let path = csv_file_path.to_str().expect("not a file");
    // pegando o nome do arquivo para ser o nome da tabela. TODO melhorar isso, muito hard-coded
    let table_name = csv_file_path.to_str().unwrap().replace(".csv", "").replace("data/", "");


    // vou criar todos os comandos para executar tudo de uma vez só no snowsql
    // 1 - criar o file format padrão
    let file_format = "ftt_csv_format".to_string();
    let file_format_query = format!("CREATE FILE FORMAT IF NOT EXISTS {file_format} TYPE = csv PARSE_HEADER = TRUE;");
    

    // 2 - vou assumir, por enquanto, que o stage já existe
    // 3 - carregar o arquivo através do put
    let put_command = "put file://".to_owned() + path + " @FTT_DATA.FTT_TEST_DATA_SCHEMA.CSV_STAGE;";

    // 4 - criar o esqueleto da tabela com CREATE USING TEMPLATE
    let create_table = format!("CREATE TABLE FTT_DATA.FTT_TEST_DATA_SCHEMA.{table_name}
    USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    WITHIN GROUP (ORDER BY order_id)
      FROM TABLE(
        INFER_SCHEMA(
          LOCATION=>'@FTT_DATA.FTT_TEST_DATA_SCHEMA.CSV_STAGE/data_basic.csv.gz',
          FILE_FORMAT=>'{file_format}'
        )
      ));").replace("\n", "").replace("\t", "");

    // 5 - copy into com os dados do arquivo carregado
    let copy_into = format!("COPY INTO FTT_DATA.FTT_TEST_DATA_SCHEMA.{table_name} FROM @FTT_DATA.FTT_TEST_DATA_SCHEMA.CSV_STAGE/data_basic.csv.gz
        FILE_FORMAT = (
        FORMAT_NAME= '{file_format}'
        )
        MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE;").replace("\n", "").replace("\t", "to");

    // execute PUT command via API call
    // PUT não pode ser por API....
    // solução: executar um processo com snowsql

    // funcionou! consigo executar queries através do processo. agora é só executar o comando PUT, com o arquivo passado na função
    // arquivo PUT executado com sucesso
    let final_query = file_format_query + &put_command + &create_table + &copy_into;
    let output = Command::new("/home/pelegolas/bin/snowsql")
        .arg("-q")
        .arg(final_query)
        .output()
        .expect("faild to execute command");
}

fn ftt_run() {}



// procura uma macro {{ table() }} e retorna o nome da tabela
fn find_and_replace_macro() {}


fn transform_columns(column_list: Vec<String>, columns_indexes: HashMap<String, i32>, table_to_transform: Table) -> Table {
    let table_copy: Table = table_to_transform.clone();
    let name = &(table_to_transform.name.to_string() + "_transformed"); 
    let mut transformed_table = Table::new(name);

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
fn create_table_json(table_to_transform: &Table, table_name: &str, table_path: &String) {
    let table_json = serde_json::to_string_pretty(&table_to_transform).unwrap();
    let path = table_path.to_owned() + table_name + ".json";
    println!("creat_table_json path: {:?}", path);
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







