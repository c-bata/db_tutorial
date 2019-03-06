use std::ops::{Range, Index, IndexMut};
use byteorder::{ByteOrder, LittleEndian};
use std::fs::OpenOptions;
use std::fs::File;
use std::vec::{Vec};
use std::process::exit;
use std::io::{Seek, SeekFrom, Read, Write};


const ID_SIZE: usize = 4;
// C strings are supposed to end with a null character.
const USERNAME_SIZE: usize = 32 + 1;
const EMAIL_SIZE: usize = 255 + 1;
const USERNAME_OFFSET: usize = ID_SIZE;
const EMAIL_OFFSET: usize = USERNAME_OFFSET + USERNAME_SIZE;
const ROW_SIZE: usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
pub const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

#[derive(Debug)]
pub struct Row {
    pub id: i32,
    pub username: String,
    pub email: String,
}

impl Row {
    fn serialize(row: &Row) -> Vec<u8> {
        let mut buf = vec![0; ROW_SIZE];
        LittleEndian::write_i32(&mut buf.index_mut(Range { start: 0, end: ID_SIZE}), row.id);
        Row::write_string(&mut buf, USERNAME_OFFSET, &row.username, USERNAME_SIZE);
        Row::write_string(&mut buf, EMAIL_OFFSET, &row.email, EMAIL_SIZE);
        return buf;
    }

    fn deserialize(buf: &Vec<u8>, pos: usize) -> Row {
        let mut bytes = vec![0; ROW_SIZE];
        bytes.clone_from_slice(buf.index(Range {
            start: pos,
            end: pos + ROW_SIZE,
        }));

        let id = LittleEndian::read_i32(bytes.as_slice());
        let username = Row::read_string(&bytes, USERNAME_OFFSET, USERNAME_SIZE);
        let email = Row::read_string(&bytes, EMAIL_OFFSET, EMAIL_SIZE);
        Row { id, username, email}
    }

    fn write_string(buf: &mut Vec<u8>, pos: usize, s: &str, length: usize) {
        let bytes = s.as_bytes();
        let mut vec = vec![0; bytes.len()];
        vec.copy_from_slice(bytes);

        // it seems to be room for performance improvements.
        let mut i = 0;
        for b in vec {
            buf[pos+i] = b;
            i += 1;
        }
        while i < length {
            buf[pos+i] = 0;
            i += 1;
        }
    }

    fn read_string(buf: &Vec<u8>, pos: usize, length: usize) -> String {
        let mut end = pos;
        while ((end - pos) < length) && (buf[end] != 0) {
            end += 1;
        }
        let mut bytes = vec![0; end - pos];
        bytes.clone_from_slice(buf.index(Range {
            start: pos,
            end,
        }));
        return String::from_utf8(bytes).unwrap();
    }
}

pub struct Table {
    pub pager: Pager,
    pub num_rows: usize,
}

impl Table {
    pub fn new(file: &str) -> Table {
        let pager = Pager::new(file);
        let num_rows = pager.file_length / ROW_SIZE;
        Table { pager, num_rows }
    }

    pub fn insert(self: &mut Table, row: &Row) {
        let bytes = Row::serialize(&row);
        let (page, mut pos) = self.row_slot(self.num_rows);
        for b in bytes {
            page[pos] = b;
            pos += 1;
        }
        self.num_rows += 1;
    }

    pub fn get_row(self: &mut Table, row_num: usize) -> Row {
        let (page, pos) = self.row_slot(row_num);
        Row::deserialize(page, pos)
    }

    fn row_slot(self: &mut Table, row_num: usize) -> (&mut Page, usize) {
        let page_num = row_num / ROWS_PER_PAGE;
        let page = self.pager.get_page(page_num);
        let row_offset = row_num % ROWS_PER_PAGE;
        let byte_offset = row_offset * ROW_SIZE;
        return (page, byte_offset)
    }

    pub fn flush_all(self: &mut Table) {
        let num_full_pages = self.num_rows / ROWS_PER_PAGE;

        for i in 0..num_full_pages {
            match self.pager.pages[i] {
                Some(_) => {
                    self.pager.flush_page(i);
                }
                None => {
                    continue
                }
            }
        }

        // There may be a partial page to write to the end of the file
        // This should not be needed after we switch to a B-tree
        let num_additional_rows = self.num_rows % ROWS_PER_PAGE;
        if num_additional_rows > 0 {
            let page_num = num_full_pages;
            if let Some(_) = self.pager.pages[page_num] {
                self.pager.flush(page_num, num_additional_rows * ROW_SIZE);
            }
        }
    }
}

type Page = Vec<u8>;

pub struct Pager {
    file: File,
    file_length: usize,
    pages: Vec<Option<Page>>,
}

impl Pager {
    fn new(file: &str) -> Pager {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file)
            .unwrap();
        let file_length = file.metadata().unwrap().len() as usize;

        let mut pages: Vec<Option<Page>> = Vec::new();
        for _i in 0..TABLE_MAX_PAGES {
            pages.push(None)
        }
        Pager { file, file_length, pages }
    }

    fn flush_page(self: &mut Pager, page_num: usize) {
        self.flush(page_num, PAGE_SIZE);
    }

    fn flush(self: &mut Pager, page_num: usize, size: usize) {
        let offset: u64 = (page_num * PAGE_SIZE) as u64;
        match self.pages[page_num].as_ref() {
            Some(page) => {
                if let Err(_e) = self.file.seek(SeekFrom::Start(offset)) {
                    println!("Error seeking: {}", _e);
                    exit(1);
                }

                if let Err(_e) = self.file.write_all(page[..size].as_ref()) {
                    println!("Error writing: {}", _e);
                    exit(1);
                }
            }
            None => {
                println!("Tried to flush null page");
                exit(1);
            }
        }
    }

    fn get_page(self: &mut Pager, page_num: usize)-> &mut Page {
        if page_num > TABLE_MAX_PAGES {
            println!("Tried to fetch page number out of bounds. {} > {}", page_num, TABLE_MAX_PAGES);
            exit(1);
        }

        if let None = self.pages[page_num] {
            let num_pages: usize = if (self.file_length % PAGE_SIZE) == 0 {
                self.file_length / PAGE_SIZE
            } else {
                // file_length が 4096 で割り切れなければ、
                // 部分的にpageが書き込まれていたかもなので +1 する
                self.file_length / PAGE_SIZE + 1
            };

            // file_length から計算したファイルに書き込まれているpage数よりも、
            // 今欲しいpageが小さいつまり必ず存在していれば、lseekしてそこから読み出す。
            if page_num <= num_pages {
                // 0 から start なので offset は単純に page_num * 4096
                let offset = page_num * PAGE_SIZE;
                self.file.seek(SeekFrom::Start(offset as u64)).unwrap();
                let mut buf = vec![0; PAGE_SIZE];
                self.file.read(buf.as_mut_slice()).unwrap();
                self.pages[page_num] = Some(buf);
            } else {
                // Page がfile内に見つからない場合は読み出さない。
                // あとで正しくfileにflushして上げる必要がある。
                self.pages[page_num] = Some(vec![0; PAGE_SIZE]);
            }
        }
        return self.pages[page_num].as_mut().unwrap();
    }
}
