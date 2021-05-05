import getopt
import sys

import pandas


def merge_process_metrics(project):
    final_defect_record = create_defect_csv(project)

    age_file_name = f"{project}/{project}_code_metrics_age.csv"
    authors_file_name = f"{project}/{project}_code_metrics_authors.csv"
    coupling_file_name = f"{project}/{project}_code_metrics_coupling.csv"

    age_csv = pandas.read_csv(age_file_name)
    authors_csv = pandas.read_csv(authors_file_name)
    coupling_csv = pandas.read_csv(coupling_file_name)

    filtered_age = filtered_dataframe(age_csv)
    filtered_authors = filtered_dataframe(authors_csv)
    filtered_coupling = filtered_dataframe(coupling_csv)

    age_authors_csv = filtered_age[["entity", "age-months"]].merge(filtered_authors[["entity", "n-authors", "n-revs"]],
                                                                   on="entity", how="left")
    age_authors_coupling = age_authors_csv[["entity", "age-months", "n-authors", "n-revs"]].merge(
        filtered_coupling[["entity", "degree", "average-revs"]], on="entity", how="left")

    ml_ready_file = final_defect_record[["entity", "no_of_defects"]].merge(
        age_authors_coupling[["entity", "age-months", "n-authors", "n-revs", "degree", "average-revs"]], on="entity",
        how="left")

    ml_ready_file.to_csv(f"{project}/{project}_ml_ready-process-metrics.csv", index=False)


def filtered_dataframe(data_frame):
    data_frame.drop(data_frame.index[data_frame['entity'].str.contains('^test.*|^tests.*')], inplace=True)
    filtered_df = data_frame[data_frame['entity'].str.contains('.*java$|.*py$') == True]

    return filtered_df


def create_defect_csv(project):
    defect_data_file_name = f"{project}/{project}.csv"
    defect_data_csv = pandas.read_csv(defect_data_file_name)
    filtered_defect_data = filtered_dataframe(defect_data_csv)
    defect_files_defect_count = filtered_defect_data.pivot_table(index=['entity'], aggfunc='size')

    defect_files_defect_count.to_csv(f"{project}/c_{project}.csv", index=True)

    final_defect_record = pandas.read_csv(f"{project}/c_{project}.csv")
    final_defect_record = final_defect_record.rename(columns={'0': 'no_of_defects'})
    final_defect_record.to_csv(f"{project}/f_{project}.csv", index=False)

    return final_defect_record


def evaluate_args():
    argument_list = sys.argv[1:]
    short_options = "hp:"
    long_options = ["help", "project="]
    try:
        arguments, values = getopt.getopt(argument_list, short_options, long_options)
    except getopt.error as err:
        print(str(err))
        sys.exit(2)
    for current_argument, current_value in arguments:
        if current_argument in ("-h", "--help"):
            print("Usage: merge_code_metrics.py --project <project folder>")
        elif current_argument in ("-p", "--project"):
            return current_value


if __name__ == '__main__':
    project_name = evaluate_args()
    if project_name:
        merge_process_metrics(project_name)
