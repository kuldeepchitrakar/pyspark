from src.jobs.wordcount.wordcount import run

def test_word_count_run(spark_session):
    expected_results = [('Hadoop', 10), ('is', 2), ('an', 1), ('elegant', 1), ('fellow.', 1), ('An', 1), ('elephant', 1) , ('gentle', 1), ('and', 1), ('mellow.', 1), ('He', 1), ('never', 1), ('gets', 1), ('mad,', 1), ('Or', 1), ('does', 1),
('anything', 1), ('bad,', 1), ('Because,', 1), ('at', 1), ('his', 1), ('core,', 1), ('he', 1), ('yellow.', 1)]
    conf = {
        'relative_path': '',
        'words_file_path': '/wordcount/resources/words.csv'
    }
    assert expected_results == run(spark_session, conf)