use std::env;
use std::io;
use std::io::Write;
use std::process::exit;
mod table;
use table::{
    Row, Table,
    LEAF_NODE_MAX_CELLS,
    leaf_node_num_cells,
    print_constants,
    print_leaf_node,
    leaf_node_key,
};

const COLUMN_USERNAME_SIZE: usize = 32;
const COLUMN_EMAIL_SIZE: usize = 255;

#[derive(Debug)]
enum Statement {
    Insert(Row),
    Select,
}

enum MetaCommandResult {
    Success,
    UnrecognizedCommand,
}

fn do_meta_command(input_buffer: &String, table: &mut Table) -> MetaCommandResult {
    match input_buffer.as_str() {
        ".exit" => {
            db_close(table);
            exit(0);
        }
        ".constants" => {
            println!("Constants:");
            print_constants();
            return MetaCommandResult::Success;
        }
        ".btree" => {
            println!("Tree:");
            print_leaf_node(table.pager.get_page(0));
            return MetaCommandResult::Success;
        }
        _ => return MetaCommandResult::UnrecognizedCommand,
    }
}

enum PrepareResultError {
    SyntaxError,
    NegativeID,
    StringTooLong,
    UnrecognizedStatement,
}

fn prepare_insert(input_buffer: &String) -> Result<Statement, PrepareResultError> {
    let splits: Vec<&str> = input_buffer.trim().split_whitespace().collect();
    if splits.len() < 4 {
        return Err(PrepareResultError::SyntaxError);
    }

    let user_id = splits[1].parse::<u32>();
    if let Err(_) = user_id {
        if splits[1].starts_with("-") {
            return Err(PrepareResultError::NegativeID);
        }
        return Err(PrepareResultError::SyntaxError)
    }
    let id = user_id.unwrap();
    let username = String::from(splits[2]);
    if username.len() > COLUMN_USERNAME_SIZE {
        return Err(PrepareResultError::StringTooLong);
    }
    let email = String::from(splits[3]);
    if email.len() > COLUMN_EMAIL_SIZE {
        return Err(PrepareResultError::StringTooLong);
    }
    let stmt = Statement::Insert(Row {
        id,
        username,
        email,
    });
    return Ok(stmt);
}

fn prepare_statement(input_buffer: &String) -> Result<Statement, PrepareResultError> {
    if input_buffer.starts_with("select") {
        return Ok(Statement::Select);
    }
    if input_buffer.starts_with("insert") {
        return prepare_insert(input_buffer);
    }
    return Err(PrepareResultError::UnrecognizedStatement);
}

enum ExecuteResult {
    Success,
    DuplicateKey,
    TableFull,
}

fn execute_insert(table: &mut Table, row: Row) -> ExecuteResult {
    let key_to_insert = row.id;
    let cursor = table.find_node(key_to_insert);
    let cell_num = cursor.cell_num;

    let root_node = table.pager.get_page(table.root_page_num);
    let num_cells = leaf_node_num_cells(&root_node) as usize;
    if num_cells >= LEAF_NODE_MAX_CELLS {
        return ExecuteResult::TableFull
    }
    if cell_num < num_cells {
        let key_at_index = leaf_node_key(root_node, cell_num as u32);
        if key_at_index == key_to_insert {
            return ExecuteResult::DuplicateKey
        }
    }
    table.find_node(key_to_insert).leaf_node_insert(row.id, &row);
    return ExecuteResult::Success;
}

fn execute_select(table: &mut Table) -> ExecuteResult {
    let mut cursor = table.start();
    while !cursor.end_of_table {
        let row = cursor.get_row();
        println!("({}, {}, {})", row.id, &row.username, &row.email);
        cursor.advance();
    }
    return ExecuteResult::Success;
}

fn execute_statement(stmt: Statement, table: &mut Table) -> ExecuteResult {
    match stmt {
        Statement::Insert(row) => {
            return execute_insert(table, row);
        }
        Statement::Select => {
            return execute_select(table);
        }
    }
}

fn db_close(table: &mut Table) {
    table.flush_all();
}

fn main() {
    let db = if let Some(file) = env::args().nth(1) {
        file
    } else {
        String::from("mydb.db")
    };
    let mut table = Table::new(db.as_str());
    let mut input_buffer: String = String::new();

    loop {
        print!("db > ");
        io::stdout().flush().unwrap();

        // read input
        input_buffer.clear();
        match io::stdin().read_line(&mut input_buffer) {
            Ok(_n) => {
                // Ignore trailing newline
                input_buffer.pop();
            }
            Err(_) => {
                println!("failed to read from stdin");
            }
        }

        if input_buffer.starts_with(".") {
            match do_meta_command(&input_buffer, &mut table) {
                MetaCommandResult::Success => {
                    continue;
                }
                MetaCommandResult::UnrecognizedCommand => {
                    println!("Unrecognized command: {}", input_buffer);
                    continue;
                }
            }
        }

        match prepare_statement(&input_buffer) {
            Ok(stmt) => {
                match execute_statement(stmt, &mut table) {
                    ExecuteResult::Success => println!("Executed."),
                    ExecuteResult::DuplicateKey => println!("Error: Duplicate key."),
                    ExecuteResult::TableFull => println!("Error: Table full"),
                };
            }
            Err(e) => match e {
                PrepareResultError::SyntaxError => {
                    println!("Syntax error. Could not parse statement.");
                }
                PrepareResultError::NegativeID => {
                    println!("ID must be positive.");
                }
                PrepareResultError::StringTooLong => {
                    println!("String is too long.");
                }
                PrepareResultError::UnrecognizedStatement => {
                    println!("Unrecognized keyword at start of '{}'", input_buffer);
                }
            },
        }
    }
}
