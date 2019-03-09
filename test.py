import os
import subprocess
import unittest

TARGET = os.getenv("TARGET", "./db")
TEST_DATABASE_FILE = os.getenv("TEST_DATABASE_FILE", "./test.db")


def run_script(commands):
    p = subprocess.Popen(
        [TARGET, TEST_DATABASE_FILE],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
        encoding='utf-8')
    input_data = "\n".join(commands) + "\n"
    try:
        outs, _ = p.communicate(input=input_data, timeout=5)
    except subprocess.TimeoutExpired:
        # See https://docs.python.org/3/library/subprocess.html#subprocess.Popen.communicate
        p.kill()
        outs, _ = p.communicate()
    lines = outs.split("\n")
    return p.returncode, lines


class MyDatabaseTest(unittest.TestCase):
    def tearDown(self):
        try:
            subprocess.run(["rm", TEST_DATABASE_FILE])
        except Exception as e:
            print(e)

    def test_inserts_and_retrieves_a_row(self):
        code, outs = run_script([
            "insert 1 user1 person1@example.com",
            "select",
            ".exit",
        ])
        self.assertEqual(code, 0)
        self.assertListEqual(outs, [
            "db > Executed.",
            "db > (1, user1, person1@example.com)",
            "Executed.",
            "db > ",
        ])

    def test_allows_inserting_strings_that_are_the_maximum_length(self):
        long_username = "a"*32
        long_email = "a"*255
        code, outs = run_script([
            f"insert 1 {long_username} {long_email}",
            "select",
            ".exit",
        ])
        self.assertEqual(code, 0)
        self.assertListEqual(outs, [
            "db > Executed.",
            f"db > (1, {long_username}, {long_email})",
            "Executed.",
            "db > ",
        ])

    def test_prints_error_message_if_strings_are_too_long(self):
        long_username = "a"*33
        long_email = "a"*256
        code, outs = run_script([
            f"insert 1 {long_username} {long_email}",
            "select",
            ".exit",
        ])
        self.assertEqual(code, 0)
        self.assertListEqual(outs, [
            "db > String is too long.",
            "db > Executed.",
            "db > ",
        ])

    def test_prints_an_error_message_if_id_is_negative(self):
        code, outs = run_script([
            "insert -1 cstack foo@bar.com",
            "select",
            ".exit",
        ])
        self.assertEqual(code, 0)
        self.assertListEqual(outs, [
            "db > ID must be positive.",
            "db > Executed.",
            "db > ",
        ])

    def test_keeps_data_after_closing_connection(self):
        code, outs = run_script([
            "insert 1 user1 person1@example.com",
            ".exit",
        ])
        self.assertEqual(code, 0)
        self.assertListEqual(outs, [
            "db > Executed.",
            "db > ",
        ])

        code, outs = run_script([
            "select",
            ".exit",
        ])
        self.assertEqual(code, 0)
        self.assertListEqual(outs, [
            "db > (1, user1, person1@example.com)",
            "Executed.",
            "db > ",
        ])

    def test_print_constants(self):
        code, outs = run_script([
            ".constants",
            ".exit",
        ])
        self.assertEqual(code, 0)
        self.assertListEqual(outs, [
            "db > Constants:",
            "ROW_SIZE: 293",
            "COMMON_NODE_HEADER_SIZE: 6",
            "LEAF_NODE_HEADER_SIZE: 10",
            "LEAF_NODE_CELL_SIZE: 297",
            "LEAF_NODE_SPACE_FOR_CELLS: 4086",
            "LEAF_NODE_MAX_CELLS: 13",
            "db > ",
        ])

    def test_allow_printing_out_the_structure_of_a_one_node_btree(self):
        ops = []
        for i in [3, 1, 2]:
            ops.append(f"insert {i} user{i} person{i}@example.com")
        ops.append(".btree")
        ops.append(".exit")
        code, outs = run_script(ops)
        self.assertEqual(code, 0)
        self.assertListEqual(outs, [
            "db > Executed.",
            "db > Executed.",
            "db > Executed.",
            "db > Tree:",
            "leaf (size 3)",
            "  - 0 : 1",
            "  - 1 : 2",
            "  - 2 : 3",
            "db > "
        ])

    def test_prints_an_error_message_if_there_is_a_duplicate_id(self):
        code, outs = run_script([
            "insert 1 user1 person1@example.com",
            "insert 1 user1 person1@example.com",
            "select",
            ".exit",
        ])
        self.assertEqual(code, 0)
        self.assertListEqual(outs, [
            "db > Executed.",
            "db > Error: Duplicate key.",
            "db > (1, user1, person1@example.com)",
            "Executed.",
            "db > ",
        ])


if __name__ == '__main__':
    unittest.main()
