use std::{array, collections::HashMap, iter, vec};

use serde::Deserialize;

// tipo de dado para a coluna
#[derive(Debug, Clone, Deserialize)]
enum ColumnDataType {
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    Null
}

// struct para os vetores de dados na coluna
#[derive(Debug)]
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
#[derive(Debug)]
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
#[derive(Debug)]
struct Column {
    index: i32,
    name: String,
    data_type: String,
    data: ColumnData,
}


fn main() {
    let filepath = "/home/pelegolas/dev/rust/ftt/src/data/data_basic.csv";

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


            println!("==================================================================");
            for record in data_remaining_records {
                for column in &mut new_table.cols {
                    if let Some(value_str) = record.get(column.index.try_into().unwrap()) {
                        let _ = column.data.push_data(value_str);
                    } else {
                        eprintln!("Warning: No value found for column '{}' at index {} in record {:?}", column.name, column.index, record);
                    }
                }
            }
  
            let table_1 = new_table;
            println!("{:?}", table_1);
            
        },
        Err(e) => eprintln!("Erro: {}", e),
    }

    let headers = ["id","name","age","city","is_valid"];
    let record = vec!["1", "Alice", "30", "New York", "True"];
    let mut vec_of_vectors: Vec<Vec<&str>> = Vec::<Vec<&str>>::new();
    for header_item in headers {
        let new_vec = vec![header_item]; 
        vec_of_vectors.push(new_vec);
    }

    for (index, row) in record.iter().enumerate() {
        let inner_vec = vec_of_vectors.get_mut(index).expect("Index out of bounds");
        inner_vec.push(row);
    }
    //println!("after adding record: {:?}", vec_of_vectors);

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

// ler o arquivo CSV e retornar a nova struct Table com os dados
fn load_csv_data(filename: &str) -> Result<HashMap<String, ColumnDataType>, Box<dyn std::error::Error>> {
    let mut rdr = csv::Reader::from_path(filename).expect("No CSV file found!");

    // getting the csv headers and creating a column struct for each one
    let headers = rdr.headers()?.clone();

    // pegando o iterador dos registros
    let mut records_iter = rdr.records();
    let mut first_data_row_types: HashMap<String, ColumnDataType> = HashMap::new();
    let mut remaining_records: Vec<csv::StringRecord> = Vec::new();

    if let Some(first_record_result) = records_iter.next() {
        let first_record: csv::StringRecord = first_record_result?;

        for (i, field) in first_record.iter().enumerate() {
            if let Some(header_name) = headers.get(i) {
                let inferred_type_for_column = infer_column_data_type(field);
                first_data_row_types.insert(header_name.to_string(), inferred_type_for_column);
            }
        }
        remaining_records.push(first_record);


    } else {
        return Ok(HashMap::new());
    }

    for record_result in records_iter {
        let record: csv::StringRecord = record_result?;
        remaining_records.push(record);
    }

    // mapped_records_and_columns já tem todas as informações dos registros, só preciso agora montar as estruturas das colunas
    let mapped_records_and_columns = manipulate_csv_data(remaining_records, &headers, first_data_row_types.clone());
    //println!("mapped_records_and_columns: {:?}", mapped_records_and_columns);

    // 1 - criar cada coluna em uma tabela nova
    let mut new_table: Table = Table::new("new_table".to_string());

    // esse for loop é para construir as colunas Column
    let mut count = 0;
    for (column_name, column_dtype) in &first_data_row_types {
            let new_column = Column {
                index: count,
                name: column_name.clone(),
                data_type: match &column_dtype {
                    ColumnDataType::Integer(_ii) => "int".to_string(),
                    ColumnDataType::Float(_ff) => "float".to_string(),
                    ColumnDataType::String(_s) => "string".to_string(),
                    ColumnDataType::Boolean(_bb) => "bool".to_string(),
                    _ => "NULL".to_string(),                  
                },
                data: match column_dtype {
                    ColumnDataType::Integer(_i) => ColumnData::Integer(Vec::<i64>::new()),
                    ColumnDataType::Float(_f) => ColumnData::Float(Vec::<f64>::new()),
                    ColumnDataType::String(_s) => ColumnData::String(Vec::<String>::new()),
                    ColumnDataType::Boolean(_b) => ColumnData::Boolean(Vec::<bool>::new()),
                    _ => ColumnData::String(Vec::<String>::new()),
                }
            };
            new_table.cols.push(new_column);
            count += 1;
    }

    // passo 2: preencher os valores, provavelmente terá que ser dentro do for loop
    for (i, column) in new_table.cols.iter_mut().enumerate() {
       if column.name.eq(&headers[i].to_lowercase()) {
        // tenho que pegar a tupla com o id desse header e iterar
        //(row, col, column name, column value, column dtype)
        for (_row, _col_index, col_name, col_value, col_dtype) in &mapped_records_and_columns {
            if col_name.eq(&column.name.to_string()) {
                let data_to_insert: ColumnData = match col_dtype {
                    ColumnDataType::Integer(_i) => ColumnData::Integer(vec![col_value.parse::<i64>().unwrap()]),
                    ColumnDataType::Float(_f) => ColumnData::Float(vec![col_value.parse::<f64>().unwrap()]),
                    ColumnDataType::String(_s) => ColumnData::String(vec![col_value.to_string()]),
                    ColumnDataType::Boolean(_b) => ColumnData::Boolean(vec![col_value.parse::<bool>().unwrap()]),
                    _ => ColumnData::String(vec!["NULL".to_string()]),
                };
                //println!("data_to_insert: {:?}", data_to_insert);
               column.data = data_to_insert;
            } else {
                continue;
            }
        }
       }
    }

    println!("===================== Aqui é a construção da tabela =====================");
    println!("new_table: {:?}", new_table);
    Ok(first_data_row_types)
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


// manipulando o csv (headers em uma estrutura e o restante em outra)
fn manipulate_csv_data(remaining_records: Vec<csv::StringRecord>, headers: &csv::StringRecord, column_datatypes: HashMap<String, ColumnDataType>) -> 
Vec<(i32, usize, String, String, ColumnDataType)> {
    let mut mapped_records_to_row_number: HashMap<i32, csv::StringRecord> = HashMap::new();
    let mut mapped_headers_to_row_number: HashMap<i32, &str> = HashMap::new();
    let mut row_num: i32 = 0;
    let mut col_num: i32 = 0;

    // (row, col, column name, column value, column dtype)
    let mut tuple_vec: Vec<(i32, usize, String, String, ColumnDataType)> = Vec::new();

    // aqui é o mapeamento dos registros (linhas) para um índice row_num
    for record in remaining_records {
        mapped_records_to_row_number.insert(row_num, record);
        row_num+=1;
    }

    // mapeamento dos headers (colunas) para um índice col_num
    for header in headers.iter() {
        mapped_headers_to_row_number.insert(col_num, header);
        col_num += 1;
    }

    // criando as tuplas dos dados, inserindo NULL para o tipo de dado da coluna
    for (key, value) in &mapped_records_to_row_number {
        for (i, item) in value.iter().enumerate() {
            // usize value is the column index, so index 0 0 means "first line, first column" and so on
            let data_to_insert: (i32, usize, String, String, ColumnDataType) = (*key, i, headers[i].to_string(), item.to_string(), ColumnDataType::Null);
            tuple_vec.push(data_to_insert);
        }
    }




    // inserindo o tipo correto de dados
    for (_key, _, column_name, column_value, column_dtype) in &mut tuple_vec {
        if let Some(dtype) = column_datatypes.get(column_name) {
            *column_dtype = dtype.clone();
        }
    }

    //println!("tuple_vec: {:?}", tuple_vec);
    tuple_vec

}





