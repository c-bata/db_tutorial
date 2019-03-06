use std::io;
use std::io::Write;
use std::process::exit;
use std::env;

mod table;
use table::{Table, Row, TABLE_MAX_ROWS};

const COLUMN_USERNAME_SIZE: usize = 32;
const COLUMN_EMAIL_SIZE: usize = 255;

#[derive(Debug)]
enum Statement {
    Insert(Row),
    Select,
}

enum MetaCommandResult {
    UnrecognizedCommand,
}

fn do_meta_command(input_buffer: &String, table: &mut Table) -> MetaCommandResult {
    match input_buffer.as_str() {
        ".exit" => {
            db_close(table);
            exit(0);
        },
        _ => {
            return MetaCommandResult::UnrecognizedCommand
        },
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
        return Err(PrepareResultError::SyntaxError)
    }

    let user_id = splits[1].parse::<i32>();
    match user_id {
        Ok(_id) => {
            if _id < 0 {
                return Err(PrepareResultError::NegativeID)
            }
        }
        Err(_) => {
            return Err(PrepareResultError::SyntaxError)
        }
    }
    let id = user_id.unwrap();
    let username = String::from(splits[2]);
    if username.len() > COLUMN_USERNAME_SIZE {
        return Err(PrepareResultError::StringTooLong)
    }
    let email = String::from(splits[3]);
    if email.len() > COLUMN_EMAIL_SIZE {
        return Err(PrepareResultError::StringTooLong)
    }
    let stmt = Statement::Insert(Row { id, username, email });
    return Ok(stmt)
}

fn prepare_statement(input_buffer: &String) -> Result<Statement, PrepareResultError> {
    if input_buffer.starts_with("select") {
        return Ok(Statement::Select)
    }
    if input_buffer.starts_with("insert") {
        return prepare_insert(input_buffer)
    }
    return Err(PrepareResultError::UnrecognizedStatement)
}

enum ExecuteResult {
    Success,
    TableFull
}

fn execute_select(table: &mut Table) -> ExecuteResult {
    for i in 0..table.num_rows {
        let row = table.get_row(i);
        println!("({}, {}, {})", row.id, &row.username, &row.email);
    }
    return ExecuteResult::Success
}

fn execute_statement(stmt: Statement, table: &mut Table) -> ExecuteResult {
    if table.num_rows >= TABLE_MAX_ROWS {
        return ExecuteResult::TableFull
    }
    match stmt {
        Statement::Insert(row) => {
            table.insert(&row);
            return ExecuteResult::Success;
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
                MetaCommandResult::UnrecognizedCommand => {
                    println!("Unrecognized command: {}", input_buffer);
                    continue;
                }
            }
        }

        match prepare_statement(&input_buffer) {
            Ok(stmt) => {
                match execute_statement(stmt, &mut table) {
                    ExecuteResult::Success => {
                        println!("Executed.")
                    }
                    ExecuteResult::TableFull => {
                        println!("Error: Table full")
                    }
                };
            }
            Err(e) => {
                match e {
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
                }
            }
        }
    }
}
