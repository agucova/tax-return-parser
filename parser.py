"""
Parse tax return documents and log matches.
"""

import csv
import glob
import re
import ray
from typing import Any, List, Pattern, Tuple
from rules import query_rules


# Files to be parsed
TARGET_FILES = r'./tax_returns/*/*/*/*'

# File to output the analysis
OUTPUT_FILE = "freq_analysis.csv"


def query_to_regex(query: str) -> Pattern:
    """Converts a boolean search query (str) to a Regular Expression.
    Please forgive the mumbo jumbo."""
    return re.compile(
        "|".join(
            map(
                lambda n: f"(?:{n})",
                query.strip().strip(".").replace("*", r"?\w+").split(" OR "),
            )
        ),
        re.IGNORECASE
    )


def check_rules(doc: str, rules: List[Tuple[str, Pattern]]) -> List[bool]:
    """ Returns the boolean value of each rule given over the document in a list."""
    result = []
    for _, query in rules:
        result.append(int(bool(query.search(doc))))

    return result


@ray.remote
def process_doc(file_path: str, rules: List[Tuple[str, Pattern]]) -> List[Any]:
    # Load file
    with open(file_path, "r", encoding="UTF-8", errors="ignore") as file:
        doc = file.read()

    # How big is the parsed file?
    doc_size = len(doc)

    # Preprocess doc. Replace newlines with spaces -> Delete tabs -> Delete repeated spaces
    doc = re.sub(" +", " ", doc.replace("\n", " ").replace("\t", ""))

    # Return the resulting row
    return list([file_path, doc_size] + check_rules(doc, rules))


def main():
    print("[INFO] Starting parser...")

    # Initiate a local ray cluster.
    ray.init(configure_logging=True, logging_format="[INFO] %(message)s")

    # Compile each rule from rules.py to its Regex counterpart.
    # Note that map is a function that applies a given function to each element of a list.
    # Here "lambda" serves as a shortcut to define a function in one line.
    rules = list(map(lambda r: (r[0], query_to_regex(r[1])), query_rules))
    # Create a new list with only the name of each rule
    name_of_loaded_rules = list(map(lambda r: r[0], rules))
    # Print the rules loaded
    print(f"[INFO] Loaded rules: {name_of_loaded_rules}")
    # Put rules in ray cluster
    rules = ray.put(rules)

    # Uses the pattern in the TARGET_FILEs constant to find a list of files.
    file_list = glob.glob(TARGET_FILES)
    if not file_list:
        print("[ERROR] No files were found in the given TARGET_FILES.")

    n_files = len(file_list)
    # Prepares a CSV writer to output the parsing
    with open(OUTPUT_FILE, "w") as f_out:
        wr = csv.writer(f_out)

        # Create the csv header
        header = ["File path", "Document size (b)"] + name_of_loaded_rules
        # Outputs the header
        wr.writerow(header)

        print(f"[INFO] Queueing {n_files} parsing tasks in the ray cluster.")
        not_ready = []
        # Queue processing for each file in the cluster
        for file_path in file_list:
            not_ready.append(process_doc.remote(file_path, rules))

        # Await finished tasks. As soon as a file is ready, write to the csv.
        n_ready = 0
        print(f"[INFO] {n_ready}/{n_files} ready.")
        while len(not_ready):
            ready_id, not_ready = ray.wait(not_ready)
            wr.writerows(list(ray.get(ready_id)))
            n_ready += 1

            if n_ready % 50 == 0:
                print(f"[INFO] {n_ready}/{n_files} ready.")

        print(f"[INFO] {n_ready}/{n_files} ready.")
        print("[INFO] Parsing finished successfully.")


if __name__ == "__main__":
    main()
