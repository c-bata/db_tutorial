use byteorder::{ByteOrder, LittleEndian};
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::{Index, IndexMut, Range};
use std::process::exit;
use std::vec::Vec;

const ID_SIZE: usize = 4;
// C strings are supposed to end with a null character.
const USERNAME_SIZE: usize = 32 + 1;
const EMAIL_SIZE: usize = 255 + 1;
const USERNAME_OFFSET: usize = ID_SIZE;
const EMAIL_OFFSET: usize = USERNAME_OFFSET + USERNAME_SIZE;
const ROW_SIZE: usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;

/*
 * Common Node Header Layout
 */
const NODE_TYPE_SIZE: usize = 1;
const NODE_TYPE_OFFSET: usize = 0;
const IS_ROOT_SIZE: usize = 1;
const IS_ROOT_OFFSET: usize = NODE_TYPE_SIZE;
const PARENT_POINTER_SIZE: usize = 4;
const PARENT_POINTER_OFFSET: usize = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const COMMON_NODE_HEADER_SIZE: usize = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

/*
 * Leaf Node Header Layout
 */
const LEAF_NODE_NUM_CELLS_SIZE: usize = 4;
const LEAF_NODE_NUM_CELLS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
const LEAF_NODE_HEADER_SIZE: usize = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

/*
 * Leaf Node Body Layout
 */
const LEAF_NODE_KEY_SIZE: usize = 4;
const LEAF_NODE_KEY_OFFSET: usize = 0;
const LEAF_NODE_VALUE_SIZE: usize = ROW_SIZE;
const LEAF_NODE_VALUE_OFFSET: usize = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const LEAF_NODE_CELL_SIZE: usize = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const LEAF_NODE_SPACE_FOR_CELLS: usize = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const LEAF_NODE_MAX_CELLS: usize = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

const LEAF_NODE_RIGHT_SPLIT_COUNT: usize = (LEAF_NODE_MAX_CELLS + 1) / 2;
const LEAF_NODE_LEFT_SPLIT_COUNT: usize = (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT;

/*
 * Internal Node Header Layout
 */
const INTERNAL_NODE_NUM_KEYS_SIZE: usize = 4;
const INTERNAL_NODE_NUM_KEYS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
const INTERNAL_NODE_RIGHT_CHILD_SIZE: usize = 4;
const INTERNAL_NODE_RIGHT_CHILD_OFFSET: usize = INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
const INTERNAL_NODE_HEADER_SIZE: usize = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE;

/*
 * Internal Node Body Layout
 */
const INTERNAL_NODE_KEY_SIZE: usize = 4;
const INTERNAL_NODE_CHILD_SIZE: usize = 4;
const INTERNAL_NODE_CELL_SIZE: usize = INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;

enum NodeType {
    Leaf,
    Internal,
}

#[derive(Debug)]
pub struct Row {
    pub id: u32,
    pub username: String,
    pub email: String,
}

impl Row {
    fn serialize(row: &Row) -> Vec<u8> {
        let mut buf = vec![0; ROW_SIZE];
        LittleEndian::write_u32(
            &mut buf.index_mut(Range {
                start: 0,
                end: ID_SIZE,
            }),
            row.id,
        );
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

        let id = LittleEndian::read_u32(bytes.as_slice());
        let username = Row::read_string(&bytes, USERNAME_OFFSET, USERNAME_SIZE);
        let email = Row::read_string(&bytes, EMAIL_OFFSET, EMAIL_SIZE);
        Row {
            id,
            username,
            email,
        }
    }

    fn write_string(buf: &mut Vec<u8>, pos: usize, s: &str, length: usize) {
        let bytes = s.as_bytes();
        let mut vec = vec![0; bytes.len()];
        vec.copy_from_slice(bytes);

        // it seems to be room for performance improvements.
        let mut i = 0;
        for b in vec {
            buf[pos + i] = b;
            i += 1;
        }
        while i < length {
            buf[pos + i] = 0;
            i += 1;
        }
    }

    fn read_string(buf: &Vec<u8>, pos: usize, length: usize) -> String {
        let mut end = pos;
        while ((end - pos) < length) && (buf[end] != 0) {
            end += 1;
        }
        let mut bytes = vec![0; end - pos];
        bytes.clone_from_slice(buf.index(Range { start: pos, end }));
        return String::from_utf8(bytes).unwrap();
    }
}

pub fn leaf_node_num_cells(node: &Page) -> u32 {
    return LittleEndian::read_u32(node.index(Range {
        start: LEAF_NODE_NUM_CELLS_OFFSET,
        end: LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE,
    }));
}

fn write_leaf_node_num_cells(node: &mut Page, num_cells: u32) {
    LittleEndian::write_u32(
        &mut node.index_mut(Range {
            start: LEAF_NODE_NUM_CELLS_OFFSET,
            end: LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE,
        }),
        num_cells,
    );
}

fn internal_node_num_keys(node: &Page) -> u32 {
    return LittleEndian::read_u32(node.index(Range {
        start: INTERNAL_NODE_NUM_KEYS_OFFSET,
        end: INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE,
    }));
}

fn write_internal_node_num_keys(node: &mut Page, num_keys: u32) {
    LittleEndian::write_u32(
        &mut node.index_mut(Range {
            start: INTERNAL_NODE_NUM_KEYS_OFFSET,
            end: INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE,
        }),
        num_keys,
    );
}

fn internal_node_right_child_pointer(node: &Page) -> u32 {
    return LittleEndian::read_u32(node.index(Range {
        start: INTERNAL_NODE_RIGHT_CHILD_OFFSET,
        end: INTERNAL_NODE_RIGHT_CHILD_OFFSET+ INTERNAL_NODE_RIGHT_CHILD_SIZE,
    }));
}

fn write_internal_node_right_child_pointer(node: &mut Page, ptr: u32) {
    LittleEndian::write_u32(
        &mut node.index_mut(Range {
            start: INTERNAL_NODE_RIGHT_CHILD_OFFSET,
            end: INTERNAL_NODE_RIGHT_CHILD_OFFSET+ INTERNAL_NODE_RIGHT_CHILD_SIZE,
        }),
        ptr,
    );
}

fn internal_node_key(node: &Page, cell_num: u32) -> u32 {
    let offset = INTERNAL_NODE_HEADER_SIZE + (cell_num as usize) * INTERNAL_NODE_CELL_SIZE;
    return LittleEndian::read_u32(node.index(Range {
        start: offset,
        end: offset + INTERNAL_NODE_KEY_SIZE,
    }));
}

fn write_internal_node_key(node: &mut Page, cell_num: u32, value: u32) {
    let offset = INTERNAL_NODE_HEADER_SIZE + (cell_num as usize) * INTERNAL_NODE_CELL_SIZE;
    LittleEndian::write_u32(
        &mut node.index_mut(Range {
            start: offset,
            end: offset + INTERNAL_NODE_KEY_SIZE,
        }),
        value,
    )
}

fn write_internal_node_child(node: &mut Page, child_num: u32, ptr: u32) {
    let num_keys = internal_node_num_keys(node);
    if child_num > num_keys {
        panic!("Tried to access child_num > num_keys");
    } else if child_num == num_keys {
        write_internal_node_right_child_pointer(node, ptr);
    } else {
        write_internal_node_key(node, child_num, ptr)
    }
}

fn get_node_type(node: &Page) -> NodeType {
    let x = node[NODE_TYPE_OFFSET];
    match x {
        0 => return NodeType::Leaf,
        1 => return NodeType::Internal,
        _ => panic!("invalid node type"),
    }
}

fn set_node_type(node: &mut Page, t: NodeType) {
    match t {
        NodeType::Leaf => node[NODE_TYPE_OFFSET] = 0,
        NodeType::Internal => node[NODE_TYPE_OFFSET] = 1,
    }
}

fn get_node_max_key(node: &Page) -> u32 {
    match get_node_type(node) {
        NodeType::Internal => {
            return internal_node_key(node, internal_node_num_keys(node) - 1);
        }
        NodeType::Leaf => {
            return leaf_node_key(node, leaf_node_num_cells(node) - 1);
        }
    }
}

fn is_node_root(node: &Page) -> bool {
    let v = node[IS_ROOT_OFFSET];
    return v != 0
}

fn set_node_root(node: &mut Page, is_root: bool) {
    if is_root {
        node[IS_ROOT_OFFSET] = 1;
    } else {
        node[IS_ROOT_OFFSET] = 0;
    }
}

fn initialize_leaf_node(node: &mut Page) {
    set_node_type(node, NodeType::Leaf);
    set_node_root(node, false);
    write_leaf_node_num_cells(node, 0);
}

fn initialize_internal_node(node: &mut Page) {
    set_node_type(node, NodeType::Internal);
    set_node_root(node, false);
    write_internal_node_num_keys(node, 0);
}

fn copy_leaf_node_cell(node_from: &Page, from_cell_num: usize, node_to: &mut Page, to_cell_num: usize) {
    let from_offset: usize = LEAF_NODE_HEADER_SIZE + (from_cell_num * LEAF_NODE_CELL_SIZE);
    let to_offset: usize = LEAF_NODE_HEADER_SIZE + (to_cell_num * LEAF_NODE_CELL_SIZE);
    for i in 0..LEAF_NODE_CELL_SIZE {
        node_to[i + to_offset] = node_from[i + from_offset]
    }
}

fn copy_page(node_from: &Page, node_to: &mut Page) {
    for i in 0..PAGE_SIZE {
        node_to[i] = node_from[i]
    }
}

fn leaf_node_value(node: &Page, cell_num: usize) -> Vec<u8> {
    let offset: usize = LEAF_NODE_HEADER_SIZE + (cell_num * LEAF_NODE_CELL_SIZE) + LEAF_NODE_KEY_SIZE;
    let mut bytes: Vec<u8> = vec![0; LEAF_NODE_VALUE_SIZE];
    bytes.clone_from_slice(node.index(Range {
        start: offset,
        end:  offset + LEAF_NODE_VALUE_SIZE,
    }));
    return bytes;
}

fn write_leaf_node_value(node: &mut Page, cell_num: usize, value: Vec<u8>) {
    let mut pos: usize = LEAF_NODE_HEADER_SIZE + (cell_num * LEAF_NODE_CELL_SIZE) + LEAF_NODE_KEY_SIZE;
    for b in value {
        node[pos] = b;
        pos += 1;
    }
}

fn write_leaf_node_key_cell(node: &mut Page, cell_num: u32, key: u32) {
    let offset = LEAF_NODE_HEADER_SIZE + LEAF_NODE_CELL_SIZE * (cell_num as usize);
    LittleEndian::write_u32(
        &mut node.index_mut(Range {
            start: offset,
            end: offset + LEAF_NODE_KEY_SIZE,
        }),
        key,
    );
}

pub fn leaf_node_key(node: &Page, cell_num: u32) -> u32 {
    let offset = LEAF_NODE_HEADER_SIZE + LEAF_NODE_CELL_SIZE * (cell_num as usize);
    let mut bytes = vec![0; LEAF_NODE_KEY_SIZE];
    bytes.clone_from_slice(node.index(Range {
        start: offset,
        end: offset + LEAF_NODE_KEY_SIZE,
    }));
    return LittleEndian::read_u32(bytes.as_slice());
}

#[derive(Debug)]
pub struct Cursor<'a> {
    pub table: &'a mut Table,
    pub page_num: usize,
    pub cell_num: usize,
    pub end_of_table: bool,
}

impl<'a> Cursor<'a> {
    fn value(&mut self) -> Vec<u8> {
        let page_num = self.page_num;

        let node = self.table.pager.get_page(page_num);
        return leaf_node_value(node, self.cell_num);
    }

    pub fn get_row(&mut self) -> Row {
        let value = self.value();
        Row::deserialize(&value, 0)
    }

    fn leaf_node_split_and_insert(&mut self, _key: u32, row: &Row) {
        /*
        Create a new node and move half the cells over.
        Insert the new value in one of the two nodes.
        Update parent or create a new parent.
        */
        let new_page_num = self.table.pager.new_leaf_node();

        for i in (0..LEAF_NODE_MAX_CELLS+1).rev() {
            let index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT;

            if i == self.cell_num {
                let destination_node: &mut Page;
                if i >= LEAF_NODE_LEFT_SPLIT_COUNT {
                    destination_node = self.table.pager.get_page(new_page_num as usize);
                } else {
                    destination_node = self.table.pager.get_page(self.page_num);
                }
                let row_data = Row::serialize(row);
                write_leaf_node_key_cell(destination_node, index_within_node as u32, row.id);
                write_leaf_node_value(destination_node, index_within_node, row_data)
            } else if i > self.cell_num {
                let old_node = self.table.pager.get_page(self.page_num).clone();
                let destination_node: &mut Page;
                if i >= LEAF_NODE_LEFT_SPLIT_COUNT {
                    destination_node = self.table.pager.get_page(new_page_num as usize);
                } else {
                    destination_node = self.table.pager.get_page(self.page_num);
                }
                copy_leaf_node_cell(&old_node, i-1, destination_node, index_within_node);
            } else {
                let old_node = self.table.pager.get_page(self.page_num).clone();
                let destination_node: &mut Page;
                if i >= LEAF_NODE_LEFT_SPLIT_COUNT {
                    destination_node = self.table.pager.get_page(new_page_num as usize);
                } else {
                    destination_node = self.table.pager.get_page(self.page_num);
                }
                copy_leaf_node_cell(&old_node, i, destination_node, index_within_node)
            }
        }

        /* Update cell count on both leaf nodes */
        write_leaf_node_num_cells(self.table.pager.get_page(self.page_num), LEAF_NODE_LEFT_SPLIT_COUNT as u32);
        write_leaf_node_num_cells(self.table.pager.get_page(new_page_num as usize), LEAF_NODE_RIGHT_SPLIT_COUNT as u32);

        if is_node_root(self.table.pager.get_page(self.page_num)) {
            return self.table.create_new_root(new_page_num);
        } else {
            println!("Need to implement updating parent after split");
            exit(1);
        }
    }

    pub fn leaf_node_insert(&mut self, key: u32, row: &Row) {
        let node = self.table.pager.get_page(self.page_num);
        let num_cells: u32 = leaf_node_num_cells(node);
        if (num_cells as usize) >= LEAF_NODE_MAX_CELLS {
            self.leaf_node_split_and_insert(key, row);
            return
        }

        let mut new_node = node.clone();
        if self.cell_num < (num_cells as usize) {
            // Make room for new ceil
            let loop_from = self.cell_num + 1;
            let loop_to = (num_cells as usize) + 1;
            for i in (loop_from..loop_to).rev() {
                // leaf_node_cell(node, i-1) の先頭から LEAF_NODE_CELL_SIZE分を leaf_node_cell(node, i)  へコピー
                // 順序が重要なのでデータをひたすらコピーしまくる。
                let offset_from: usize = LEAF_NODE_HEADER_SIZE + ((i-1) * LEAF_NODE_CELL_SIZE);
                let offset_to: usize = LEAF_NODE_HEADER_SIZE + (i * LEAF_NODE_CELL_SIZE);
                new_node[offset_to..offset_to+LEAF_NODE_CELL_SIZE].copy_from_slice(&node[offset_from..offset_from+LEAF_NODE_CELL_SIZE])
            }
        }

        write_leaf_node_num_cells(&mut new_node, num_cells+1);
        write_leaf_node_key_cell(&mut new_node, self.cell_num as u32, key);
        let value = Row::serialize(&row);
        write_leaf_node_value(&mut new_node, self.cell_num, value);

        node.copy_from_slice(&new_node);
    }

    pub fn advance(&mut self) {
        let page_num = self.page_num;
        let node = self.table.pager.get_page(page_num);

        self.cell_num += 1;
        let num_cells = leaf_node_num_cells(node) as usize;
        if self.cell_num >= num_cells {
            self.end_of_table = true;
        }
    }
}

#[derive(Debug)]
pub struct Table {
    pub pager: Pager,
    pub root_page_num: usize,
}

impl<'a> Table {
    pub fn new(file: &str) -> Table {
        let mut pager = Pager::new(file);
        let root_page_num: usize = 0;

        if pager.num_pages == 0 {
            // New database file. Initialize page 0 as leaf node.
            let root_node = pager.get_page(0);
            initialize_leaf_node(root_node);
        }

        Table { pager, root_page_num }
    }

    fn create_new_root(&mut self , right_child_page_num: u32) {
        /*
        Handle splitting the root.
        Old root copied to new page, becomes left child.
        Address of right child passed in.
        Re-initialize root page to contain the new root node.
        New root node points to two children.
        */

        let left_child_page_num = self.pager.get_unused_page_num();
        self.pager._load_page(right_child_page_num as usize);
        self.pager._load_page(left_child_page_num as usize);
        self.pager._load_page(self.root_page_num);

        let mut_left_child = self.pager.get_page(left_child_page_num as usize);
        set_node_root(mut_left_child, false);

        /* Left child has data copied from old root */
        let left_child = self.pager.get_immutable_page(
            left_child_page_num as usize
        ).unwrap();
        let root = self.pager.get_page(self.root_page_num);
        copy_page(&left_child, root);

        /* Root node is a new internal node with one key and two children */
        initialize_internal_node(root);
        set_node_root(root, true);
        write_internal_node_num_keys(root, 1);
        write_internal_node_child(root, 0, left_child_page_num);
        let left_child_max_key: u32 = get_node_max_key(&left_child);
        write_internal_node_key(root, 0, left_child_max_key);
        write_internal_node_right_child_pointer(root, right_child_page_num);
    }

    pub fn start(&mut self) -> Cursor {
        let page_num = self.root_page_num;
        let cell_num = 0;

        let root_node = self.pager.get_page(self.root_page_num);
        let num_cells = leaf_node_num_cells(root_node);
        let end_of_table = num_cells == 0;

        Cursor {
            table: self,
            page_num,
            cell_num,
            end_of_table,
        }
    }

    pub fn find_node(&mut self, key: u32) -> Cursor {
        let page_num = self.root_page_num;
        let root_node = self.pager.get_page(self.root_page_num);

        match get_node_type(&root_node) {
            NodeType::Leaf => {
                return self.leaf_node_find(page_num as u32, key);
            }
            NodeType::Internal => {
                panic!("Need to implement searching an internal node");
            }
        }
    }

    fn leaf_node_find(&mut self, page_num: u32, key: u32) -> Cursor {
        let node = self.pager.get_page(page_num as usize);
        let num_cells = leaf_node_num_cells(&node);

        let mut min_index = 0;
        let mut one_past_max_index = num_cells;

        while one_past_max_index != min_index {
            let index = (min_index + one_past_max_index) / 2;
            let key_at_index = leaf_node_key(node, index);
            if key == key_at_index {
                return Cursor {
                    table: self,
                    page_num: page_num as usize,
                    cell_num: index as usize,
                    end_of_table: false,
                }
            }
            if key < key_at_index {
                one_past_max_index = index;
            } else {
                min_index = index + 1;
            }
        }
        return Cursor {
            table: self,
            page_num: page_num as usize,
            cell_num: min_index as usize,
            end_of_table: false
        }
    }

    pub fn flush_all(self: &mut Table) {
        for i in 0..self.pager.num_pages {
            match self.pager.pages[i] {
                Some(_) => {
                    self.pager.flush_page(i);
                }
                None => continue,
            }
        }
    }
}

type Page = Vec<u8>;

#[derive(Debug)]
pub struct Pager {
    file: File,
    file_length: usize,
    num_pages: usize,
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
        let num_pages = file_length / PAGE_SIZE;

        if file_length % PAGE_SIZE != 0 {
            println!("Db file is not a whole number of pages. Corrupt file.");
            exit(1);
        }
        let mut pages: Vec<Option<Page>> = Vec::new();
        for _i in 0..TABLE_MAX_PAGES {
            pages.push(None)
        }
        Pager {
            file,
            file_length,
            num_pages,
            pages,
        }
    }

    fn flush_page(self: &mut Pager, page_num: usize) {
        let offset: u64 = (page_num * PAGE_SIZE) as u64;
        match self.pages[page_num].as_ref() {
            Some(page) => {
                if let Err(_e) = self.file.seek(SeekFrom::Start(offset)) {
                    println!("Error seeking: {}", _e);
                    exit(1);
                }

                if let Err(_e) = self.file.write_all(page.as_ref()) {
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

    fn get_unused_page_num(self: &Pager) -> u32 {
        // Until we start recycling free pages, new pages will always
        // go onto the end of the database file
        return self.num_pages as u32;
    }

    fn new_leaf_node(self: &mut Pager) -> u32 {
        let new_page_num = self.get_unused_page_num();
        let new_node = self.get_page(new_page_num as usize);
        initialize_leaf_node(new_node);
        return new_page_num;
    }

    pub fn _load_page(self: &mut Pager, page_num: usize) {
        let num_pages: usize = if (self.file_length % PAGE_SIZE) == 0 {
            self.file_length / PAGE_SIZE
        } else {
            self.file_length / PAGE_SIZE + 1
        };

        if page_num <= num_pages {
            let offset = page_num * PAGE_SIZE;
            self.file.seek(SeekFrom::Start(offset as u64)).unwrap();
            let mut buf = vec![0; PAGE_SIZE];
            self.file.read(buf.as_mut_slice()).unwrap();
            self.pages[page_num] = Some(buf);
        } else {
            self.pages[page_num] = Some(vec![0; PAGE_SIZE]);
        }
    }

    pub fn get_immutable_page(self: &Pager, page_num: usize) -> Option<Page> {
        if page_num > TABLE_MAX_PAGES {
            println!(
                "Tried to fetch page number out of bounds. {} > {}",
                page_num, TABLE_MAX_PAGES
            );
            exit(1);
        }
        return self.pages[page_num].clone()
    }

    pub fn get_page(self: &mut Pager, page_num: usize) -> &mut Page {
        if page_num > TABLE_MAX_PAGES {
            println!(
                "Tried to fetch page number out of bounds. {} > {}",
                page_num, TABLE_MAX_PAGES
            );
            exit(1);
        }

        if let None = self.pages[page_num] {
            self._load_page(page_num);
        }
        return self.pages[page_num].as_mut().unwrap();
    }
}

pub fn print_leaf_node(node: &Page) {
    let num_cells = leaf_node_num_cells(node);
    println!("leaf (size {})", num_cells);
    for i in 0..num_cells {
        let key = leaf_node_key(node, i);
        println!("  - {} : {}", i, key);
    }
}

pub fn print_constants() {
    println!("ROW_SIZE: {}", ROW_SIZE);
    println!("COMMON_NODE_HEADER_SIZE: {}", COMMON_NODE_HEADER_SIZE);
    println!("LEAF_NODE_HEADER_SIZE: {}", LEAF_NODE_HEADER_SIZE);
    println!("LEAF_NODE_CELL_SIZE: {}", LEAF_NODE_CELL_SIZE);
    println!("LEAF_NODE_SPACE_FOR_CELLS: {}", LEAF_NODE_SPACE_FOR_CELLS);
    println!("LEAF_NODE_MAX_CELLS: {}", LEAF_NODE_MAX_CELLS);
}

#[cfg(test)]
mod tests {
    use crate::table::{LEAF_NODE_HEADER_SIZE, PAGE_SIZE, LEAF_NODE_VALUE_SIZE};
    use crate::table::{leaf_node_num_cells, leaf_node_value, leaf_node_key};
    use crate::table::{write_leaf_node_num_cells, write_leaf_node_value, write_leaf_node_key_cell};

    #[test]
    fn test_node_num_cells() {
        let mut node = vec![0; LEAF_NODE_HEADER_SIZE];
        write_leaf_node_num_cells(&mut node, 5);
        let num_cells = leaf_node_num_cells(&node);
        assert_eq!(num_cells, 5);
    }

    #[test]
    fn test_leaf_node_value() {
        let mut something_value: Vec<u8> = vec![0; LEAF_NODE_VALUE_SIZE];
        for (i, b) in "Hello World".as_bytes().iter().enumerate() {
            something_value[i] = *b;
        }
        let expected = something_value.clone();

        let mut node = vec![0; PAGE_SIZE];
        write_leaf_node_key_cell(&mut node, 0, 1);
        write_leaf_node_value(&mut node, 0, something_value);
        assert_eq!(leaf_node_value(&node, 0), expected);
        assert_eq!(leaf_node_key(&node, 0), 1);
    }
}
