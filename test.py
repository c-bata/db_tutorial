import subprocess
import unittest


def run_script(commands, filename="./test.db"):
    p = subprocess.Popen(
        ["./target/debug/hello", filename],
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
            subprocess.run(["rm", "test.db"])
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


if __name__ == '__main__':
    unittest.main()
