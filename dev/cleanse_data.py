import sqlite3
import pandas as pd
import ast
import numpy as np
import os
import logging


os.chdir('/Users/alexa/Documents/subscriber-pipeline-starter-kit/')

logging.basicConfig(filename=".dev/cleanse_db.log", 
                    format="%(asctime)s - %(name)s - %(levelname)s = %(message)s",
                    filemode="w",
                    level= logging.DEBUG,
                    force=True)

logger = logging.getLogger(__name__)



def cleanse_student_table(df):
    now = pd.to_datetime('now')

    df['age'] = (now - pd.to_datetime(df['dob'])).astype('<m8[Y]')
    df['age_group'] = np.int64((df['age']/10))*10

    # Flatten the data in the column contact info for easier access or separation
    df['contact_info'] = df['contact_info'].apply(lambda x: ast.literal_eval(x))
    explode_contact = pd.json_normalize(df['contact_info'])


    df = pd.concat([df.drop('contact_info', axis=1), explode_contact], axis=1)


    # Split Contact Info Column by commas into their own columns
    split_address = df['mailing_address'].str.split(',', expand=True)
    split_address.columns = ['street', 'city', 'state', 'zip_code']

    df = pd.concat([df.drop('mailing_address', axis=1), split_address], axis=1)


    # Change the Type of value for each column based on table data
    df['job_id'] = df['job_id'].astype(float)
    df['num_course_taken'] = df['num_course_taken'].astype(float)
    df['current_career_path_id'] = df['current_career_path_id'].astype(float)
    df['time_spent_hrs'] = df['time_spent_hrs'].astype(float)


    # # Handling Missing Data
    # Checks for any null values in the 'num course taken' column and saves it into a new variable 
    missing_course_taken = df[df[['num_course_taken']].isnull().any(axis=1)]
    display(missing_course_taken)


    # Create a new dataframe called missing data
    missing_data = pd.DataFrame()

    #Merge missing data and missing course taken data declared above
    missing_data = pd.concat([missing_data, missing_course_taken])

    # Drop the column num course taken
    df = df.dropna(subset=['num_course_taken'])


    # Check for null values in 'job id' column
    missing_job_id = df[df[['job_id']].isnull().any(axis=1)]


    # Merge missing data and missing job id dfs
    missing_data = pd.concat([missing_data, missing_job_id])

    # Drop the job id column
    df = df.dropna(subset=['job_id'])


    #Check for null values in current career path id column 
    missing_career_path_id = df[df[['current_career_path_id']].isnull().any(axis=1)]
    display(missing_career_path_id)


    # Check for unique values in column current career path id
    df['current_career_path_id'].unique()


    # Use numpy function to check where there is null values in specified cols
    df['current_career_path_id'] = np.where(df['current_career_path_id'].isnull(), 0, df['current_career_path_id'])
    df['time_spent_hrs'] = np.where(df['time_spent_hrs'].isnull(), 0, df['time_spent_hrs'])
    return(df, missing_data)






# # Working with Courses Table
# Display Course table columns
def cleanse_courses_table(df):
    not_applicable = {'career_path_id': 0,
                    'career_path_name': 'not applicable',
                    'hours_to_complete': 0}

    df.loc[len(df)] = not_applicable
    return(df)





## Working with Student Jobs Table
def cleanse_student_jobs(df):
    return(df.drop_dupliacates())



def test_nulls(df):
    df_missing = df[df.isnull().any(axis=1)]
    cnt_missing = len(df_missing)

    try:
        assert cnt_missing == 0, "There is " + str(cnt_missing) + "nulls in the table."
        except AssertionError as ae:
            logger.exception(ae)
            raise(ae)
        else:
            print("No null rows found")




def test_schema(local_df, db_df):
    errors = 0
    for col in db_df:
        try:
            if local_df[col].dtypes != db_df[col]:
                errors += 1
        except NameError as ne:
            logger.exception(ne)
            raise(ne)
    if errors > 0:
        assert_err_msg = str(errors) + "column(s) dtypes aren't the same."
        logger.exception(assert_err_msg)
    assert errors == 0, assert_err_msg



def test_num_cols(local_df, db_df):
    try:
        assert len(local_df.columns == len(db_df.columns))
    except AssertionError as ae:
        logger.exception(ae)
        raise ae
    else: 
        print("Number of columns are the same.")

    student_table = students.current_career_path_id.unique()
    is_subset = np.isin(student_table, course.career_path_id.unique())
    missing_id = student_table[~is_subset]

    try:
        assert len(missing_id) == 0, "Missing career_path_id(s): " + str(list(missing_id)) + "in 'career_paths' table"
    except AssertionError as ae:
        logger.exception(ae)
        raise ae 
    else:
        print("All career_path_ids are present.")




def test_for_job_id(students, student_jobs):
    student_table = students.job_id.unique()
    is_subset = np.isin(student_table, student_jobs.job_id.unique())
    missing_id = student_table[~is_subset]

    try:
        assert len(missing_id) == 0, "Missing job_id(s): " + str(list(missing_id)) + "in 'student_jobs' table"
    except AssertionError as ae:
        logger.exception(ae)
        raise ae 
    else:
        print("All job_ids are present.")




def main():
    logger.info("Start Log")

    with open('.dev/.changelog.md' 'a+') as f:
        lines = f.read_lines()
    if len(lines) == 0:
        next_ver = 0
    else:
        next_ver = int(lines[0].split('.')[2][0])+1
    
    connect = sqlite3.connect('./dev/cademycode.db')
    students = pd.read_sql_query("SELECT * FROM cademycode_students", connect)
    courses = pd.read_sql_query("SELECT * FROM cademycode_courses", connect)
    student_jobs = pd.read_sql_query("SELECT * FROM cademycode_student_jobs", connect)
    connect.close()

    try:
        connect = sqlite3.connect('./prod/cademycode_cleansed.db')
        clean_db = pd.read_sql_query("SELECT * FROM cademycode_aggregated", connect)
        missing_db = pd.read_sql_query("SELECT * FROM incomplete_data", connect)
        connect.close()

        new_students = students[~np.isin(students.uuid.unique(), clean_db.uuid.unique())]
    except:
        new_students = students
        clean_db = []

    clean_new_students, missing_data = cleanse_student_table(new_students)

    try:
        new_missing_data = missing_data[~np.isin(missing_data.uuid.unique(), missing_db.uuid.unique())]
    except:
        new_missing_data = missing_data
    
    if len(new_missing_data) > 0:
        sqlite_connection = sqlite3.connect('./dev/cademycode_cleansed.db')
        missing_data.to_sql('incomplete_data', sqlite_connection, if_exists='append', index=False)
        sqlite_connection.close()
    
    if len(clean_new_students) > 0:
        clean_career_paths = cleanse_career_path(career_path)
        clean_student_jobs = cleanse_student_jobs(student_jobs)


        test_for_job_id(clean_new_students, clean_student_jobs)
        test_for_path_id(clean_new_students, clean_courses_table)

        df_clean = clean_new_students.merge(clean_courses_table, 
                left_on='current_career_path_id', 
                right_on='career_path_id',
                how='left')

        df_clean = df_clean.merge(
                clean_student_jobs, 
                on='job_id', 
                how='left')
        
        if len(clean_db) > 0:
            test_num_cols(df_clean, clean_db)
            test_schema(df_clean, clean_db)
        test_nulls(db_clean)

        sqlite_connection = sqlite3.connect('./dev/cademycode_cleansed.db')
        df_clean.to_sql('cademycode_aggregated', sqlite_connection, if_exists='append', index=False)
        sqlite_connection.close()


        clean_db.to_csv('./dev/cademycode_cleansed.csv')

        new_lines = [
            '## 0.0' + str(next_ver) + '\n' +
            '### Added\n' +
            '- ' + str(len(df_clean)) + ' more data to database of raw data\n' + 
            '- ' + str(len(new_missing_data)) + ' new missing data to incomplete_data table\n' +
            '\n'
         ]
         w_lines = ''.join(new_lines + lines)
         with open('./dev/changelog.md', 'w') as f:
            for line in w_lines:
                f.write(line)
    else:
        print("no new data")
        logger.info('no new data')
    logger.info("End Log")


if __name__ == "__main__":
    main()
