"""
Parse tax return documents and log matches.
"""

import csv
import glob
import re
import regex as re2
import ray
import sys
from typing import Any, List, Pattern, Tuple, Union
from rules import query_rules
from alive_progress import alive_bar

# Files to be parsed
# Use this constant for the EDGAR dataset:
# TARGET_FILES = r"./tax_returns/*/*/*/*"
# TARGET_FILES = r"/Volumes/G-DRIVE mobile USB-C/EDGAR/10-X_C_2006-2010/2009/*/*"
TARGET_FILES = r"./tax_returns/*"

# File to output the analysis
OUTPUT_FILE = "freq_analysis.csv"

# Whether to reduce scope to just the MD&A (experimental)
filter_mda = True


def query_to_regex(query: str) -> Pattern:
    """Converts a boolean search query (str) to a Regular Expression.
    Please forgive the mumbo jumbo."""
    return re.compile(
        "|".join(
            map(
                lambda n: r"\b" + n + r"\b",
                query.strip().strip(".").replace("*", r"?\w+").split(" OR "),
            )
        )
    )


def check_rules(doc: str, rules: List[Tuple[str, Pattern]]) -> List[int]:
    """ Returns the boolean value of each rule given over the document in a list."""
    result = []
    for _, query in rules:
        result.append(int(bool(query.search(doc))))

    return result


@ray.remote
def process_doc(file_path: str, rules: List[Tuple[str, Pattern]], filter_mda_re: Union[Pattern, bool] = False) -> List[Any]:
    # Load file
    with open(file_path, "r", encoding="UTF-8", errors="ignore") as file:
        doc = file.read()

    # How big is the parsed file?
    doc_size = len(doc)

    # Preprocess doc. Replace newlines with spaces -> Delete tabs -> Delete repeated spaces -> All document to lowercase
    doc = re.sub(" +", " ", doc.replace("\n", " ").replace("\t", "")).lower()

    # If the MD&A filter was given
    if filter_mda_re:
        # Filter the document
        mda_match = filter_mda_re.search(doc)
        # If no MD&A found, skip
        if not mda_match:
            return []

        # Get the filtered doc
        doc = mda_match.group()

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
        sys.exit()

    # Save the length of the file
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

        # Compile the MDA Filter regex if needed
        filter_mda_re = re2.compile(r"(?<=item 7)(.*)(?=item 8)", re2.DOTALL, re2.REVERSE) if filter_mda else False

        # Queue processing for each file in the cluster
        n_loaded = 0
        with alive_bar(n_files) as step:
            for file_path in file_list:
                not_ready.append(process_doc.remote(file_path, rules, filter_mda_re))
                n_loaded += 1
                step()
        print(f"[INFO] Queued {n_files} tasks.")

        # Await finished tasks. As soon as a file is ready, write to the csv.
        n_ready = 0
        print(f"[INFO] Processing {n_files} files in the ray cluster.")
        with alive_bar(n_files, force_tty=True) as step:
            while len(not_ready):
                ready_id, not_ready = ray.wait(not_ready)
                ready = ray.get(ready_id)
                if ready:
                    wr.writerows(ready)
                    step()
                n_ready += 1


        print(f"[INFO] {n_ready}/{n_files} processed.")
        print("[INFO] Parsing finished successfully.")


if __name__ == "__main__":
    main()
