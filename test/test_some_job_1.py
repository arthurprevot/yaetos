""" Example test file, not functional. Just to show what it would look like."""
import some_job_1

class test_some_job_1():
    def test_run(sc):
        input_df1 = [
            {'field1': 1234, 'field2': 4321},
            {'field1': 1234, 'field2': 4321},
            {'field1': 1234, 'field2': 4321},
            ]
        input_df2 = [
            {'field1': 1234, 'field2': 4321},
            {'field1': 1234, 'field2': 4321},
            {'field1': 1234, 'field2': 4321},
            ]
        expected = [
            {'field1': 1234, 'field2': 4321, 'field3': 87},
            {'field1': 1234, 'field2': 4321, 'field3': 87},
            {'field1': 1234, 'field2': 4321, 'field3': 87},
            ]

        actual = some_job_1().run(input_df1, input_df2)
        assert actual == expected

    def test_some_record_change():

        input_rec = {'field1': 1234, 'field2': 4321},
        expected = {'field1': 1234, 'field2': 4321, 'field4': 87},

        actual = some_record_change(input_rec)
        assert actual == expected
