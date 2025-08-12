use std::{any::Any, collections::{hash_map, HashMap}};

use field_count::FieldCount;
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
    //let data = read_file_serde(filepath);

    /* temos os dados do CSV em um vetor de structs, e agora?
     "rodar o modelo" significa executar transformações nos dados
     então eu preciso:
        - iterar sobre todos os rows
        - aplicar a lógica em cada um
        - criar os novos dados

    vamos do básico. primeiro vamos selecionar apenas alguns campos específicos como transformação

    */

    // simulação de um arquivo de modelo sql. em breve precisarei de um sql parser
    //let sql_command = "SELECT id, name, age FROM data_basic;";
    //identify_sql_command_columns(sql_command.to_string());
    

    match load_csv_data(filepath) {
        Ok(data) => {
            let mut data_headers: HashMap<String, ColumnDataType> = HashMap::new();
            data_headers.clone_from(&data);

            //println!("final result: {:#?}", data_headers);

            //build_table_structure(data_headers);
        },
        Err(e) => eprintln!("Erro: {}", e),
    }

}


// reads CSV and returns a Vec with StringRecords
fn read_file(filename: &str) -> /*Result<(), Box<dyn std::error::Error>>*/ Vec<csv::StringRecord> {
    let mut rdr = csv::Reader::from_path(filename).expect("need a csv file");
    let mut file_contents = Vec::new();

    for result in rdr.records() {
        let record = result.unwrap();
        file_contents.push(record);
    }

    file_contents
}

// para transformar os dados, eu passo o comando SQL como parametro e recebo um vetor de structs novamente, com os dados atualizados (transformados)
/*
    // vamos supor que ja temos o parser e que ja sabemos que precisamos selecionar colunas especificas. talvez fosse melhor guardar os dados do csv column-oriented.

}
*/

// ja temos os dados em uma struct em formato colunar, ou seja, cada coluna tem o seu vetor de dados dentro da struct
// agora, preciso criar uma função que receba o comando SQL (assumindo que o aprser já sabe o que precisa fazer) e aplique as transformações
//fn transform_data(sql_command: String) -> Vec<MyRecord> {
/*
passos
1- identificar as colunas do comando SQL. São as palavras entre o SELECT e o FROM, separadas por vírgula. Como fazer isso? Regex?
passo 1 feito, está sub otimizado mas para um MVP está bom

2 - 
*/

//}

// retorna um vec<string> pois preciso enumerar/separar cada coluna
fn identify_sql_command_columns(sql_command: String) -> Vec<String> {
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
    //println!("headers: {:?}", headers);
    //println!("load_csv_data remaining_records: {:?}", remaining_records);

    // mapped_records_and_columns já tem todas as informações dos registros, só preciso agora montar as estruturas das colunas
    let mapped_records_and_columns = manipulate_csv_data(remaining_records, headers, first_data_row_types.clone());

    // 1 - criar cada coluna em uma tabela nova
    let mut new_table: Table = Table::new("new_table".to_string());

    // esse for loop é para construir as colunas Column
    for (column_name, column_dtype) in &first_data_row_types {
            let mut count = 0;
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
    }

    // passo 2: preencher os valores, provavelmente terá que ser dentro do for loop

    println!("===================== Aqui é a construção da tabela =====================");
    println!("new_table: {:?}", new_table);
    // a partir desse ponto, temos os headers e os registros restantes. qual o próximo passo?
    /*
        bom, posso pegar a lógica atual da main (que cria a tabela vazia, com as colunas do tipo certo) e preencher o dados. O problema é que sendo um hashmap, a ordem
        é aleatória.
        mas eu posso procurar pelo nome da coluna...
        melhor criar a coluna e já preencher com o dado. é isso
    */


    /*for (column_name, column_dtype) in &first_data_row_types {

        if let ColumnDataType::Integer(_i) = column_dtype {
            //let new_column = Column {
             //   name: column_name.clone(),
            //    data_type: "i64".to_string(),
            //    data: ColumnData::Integer(Vec::<i64>::new())
            //};
            // 2 - preencher o vetor dos dados
            //if let ColumnDataType::Integer(vec_ref) = &mut new_column.data {
            //    vec_ref.push(colu)
            //    new_table.cols.push(new_column);
            //}
            // aqui que preciso acessar os registros mapeados em tuplas 
        }
    }*/

    //println!("first_data_row_types: {:?}", first_data_row_types);
    Ok(first_data_row_types)
}

// fazer o parsing de valor de acordo com o ColumnDataType
/*fn parse_column_value(tuple_value: ColumnDataType) -> ColumnData {
    match tuple_value {
        ColumnDataType::Integer(i) => ColumnData::Integer(i),
        ColumnDataType::Float(f) => ColumnData::Float(f),
        ColumnDataType::String(s) => ColumnData::String(s),
        ColumnDataType::Boolean(b) => ColumnData::Boolean(b),
        _ => ColumnData::String("NULL".to_string()),        
    }
}*/

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
fn manipulate_csv_data(remaining_records: Vec<csv::StringRecord>, headers: csv::StringRecord, column_datatypes: HashMap<String, ColumnDataType>) -> 
Vec<(i32, String, String, ColumnDataType)> {
    let mut mapped_headers_to_row_number: HashMap<i32, csv::StringRecord> = HashMap::new();
    let mut row_num: i32 = 0;

    // (key, column name, column value, column dtype)
    let mut tuple_vec: Vec<(i32, String, String, ColumnDataType)> = Vec::new();

    for record in remaining_records {
        mapped_headers_to_row_number.insert(row_num, record);
        row_num+=1;
    }

    // criando as tuplas dos dados, inserindo NULL para o tipo de dado da coluna
    for (key, value) in &mapped_headers_to_row_number {
        for (i, item) in value.iter().enumerate() {
            let data_to_insert: (i32, String, String, ColumnDataType) = (*key, headers[i].to_string(), item.to_string(), ColumnDataType::Null);
            //println!("data to insert: {:?}", data_to_insert);
            tuple_vec.push(data_to_insert);
        }
    }


    // inserindo o tipo correto de dados
    for (_key, column_name, column_value, column_dtype) in &mut tuple_vec {
        /*for (name, dtype) in &column_datatypes {
            // se o nome da coluna dentro da tupla for igual ao nome da coluna do hashmap, então pega o columndatatype do hashmap e insere na tuple
            if name.eq(column_name) {
                *column_dtype = dtype.clone();
                //println!("column_dtype: {:?}", column_dtype);
            }
            
        }*/
        if let Some(dtype) = column_datatypes.get(column_name) {
            *column_dtype = dtype.clone();
            //update_column_dtype_variant(column_name.to_string());
            //println!("column_dtype: {:?}", column_dtype);
        }
    }

    //println!("tuple_vec: {:?}", tuple_vec);

    tuple_vec

}


// processo reverso da inferencia de tipo: quero extrair qual é o tipo da coluna para poder criar o meu vetor desse tipo extraido.
// porém acho que terei que usar um tipo genérico, pois eu não sei qual tipo irei retornar, só depois de avaliar.
/* fn extract_column_data_type<T>(column: ColumnDataType) -> &T {
    if let ColumnDataType::Integer(i) = column {
        return i;
    }
} 
*/

// criar a tabela a partir do hashmap
/* 
fn build_table_structure<T>(table_data: HashMap<String, ColumnDataType>, data_type: T) {
    let table: Table<T> = Table::new(); 
    let mut column_names = Vec::<String>::new();
    let mut column_types = Vec::<&ColumnDataType>::new();

    for (tname, dtype) in table_data {
        //let data_type = dtype;

        // criando a coluna de dados
        if let ColumnDataType::Integer(i) = data_type {
            println!("{:?} is integer and it comes from column {:?}", i, tname);
            
            table.cols.push(Column { name: tname.clone(), data_type: "Int".to_string(), data: });
            /*let column = Column {
                name: tname.clone(),
                data_type: "Int".to_string(),
                data: Vec::<i64>::new()
            };*/
        }

        if let ColumnDataType::Float(f) = data_type {
            println!("{:?} is float and it comes from column {:?}", f, &tname);
        }

        if let ColumnDataType::String(ref s) = data_type {
            println!("{:?} is string and it comes from column {:?}", s, &tname);
        }

        if let ColumnDataType::Boolean(b) = data_type {
            println!("{:?} is boolean and it comes from column {:?}", b, &tname);
        }

    } */
    
    /*
    for row in table_data {
        //println!("row from build_table_structure: {:?}", row);
        //column_names.push(row.1.0.to_string());
        //column_types.push(row.1.1);
        let column_data_type = row.1;

        let new_column = Column{
            name: row.0.to_string(),
            data_type: row.1,
            data: Vec::<ColumnDataType>::new()
        };

        table.cols.push(new_column);
    }
    
} */


// criar um vector a partir do ColumnDataType 
/*
fn build_vector_from_column_data_type<T>(data_type: ColumnDataType) -> Vec<T> {

    let default_return_value = Vec::<String>::new();
    if data_type.eq(ColumnDataType::String()) {
        println!("ok?");
        return Vec::<String>::new();
    }

    default_return_value
}

*/
