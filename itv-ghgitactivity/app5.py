import os
from util import get_spark_session
from read import from_files
from process import transform
from write import to_files


def main():
    env = os.environ.get('ENVIRON')
    spark = get_spark_session(env, 'GitHub Activity - Getting Started')
    src_dir = os.environ.get('SRC_DIR')
    #file_pattern = os.environ.get('FILE_PATTERN')
    file_pattern = f"{os.environ.get('SRC_FILE_PATTERN')}-*"
    src_file_format = os.environ.get('SRC_FILE_FORMAT')
    tgt_dir = os.environ.get('TGT_DIR')
    tgt_file_format = os.environ.get('TGT_FILE_FORMAT')
    #spark = get_spark_session(env, 'GitHub Activity - Reading and Writing Data')
    #spark.sql('SELECT current_date').show()
    #df = from_files(spark, 'data', 'github-activity-2021-*.json', 'json')
    df = from_files(spark, src_dir, file_pattern, src_file_format)
    df_transformed = transform(df)
    to_files(df_transformed, tgt_dir, tgt_file_format)

if __name__ == '__main__':
    main()