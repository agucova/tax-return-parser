using Glob
using CSV
using Distributed
using ProgressMeter
using ThreadTools: tmap

@everywhere using Pipe: @pipe
@everywhere include("rules.py")


# Files to be parsed
# Use this constant for the EDGAR dataset:
# TARGET_FILES = r"./tax_returns/*/*/*/*"
TARGET_FILES = raw"./tax_returns/*"

# File to output the analysis
OUTPUT_FILE = "freq_analysis.csv"

# Whether to reduce scope to just the MD&A (experimental)
filter_mda = false

function query_to_regex(query::String)::Regex
    return @pipe query |>
    strip(_) |>
    strip(_, '.') |>
    replace(_, "*" => raw"?\w+") |>
    split(_, " OR ") |>
    map(n -> raw"\b" * n * raw"\b", _) |>
    join(_, "|") |>
    Regex(_, "i")
end

@everywhere function check_rules(doc::AbstractString, rules::Array{Tuple{String, Regex}})::Array{Integer}
    result::Array{Integer} = []
    for (_, query) in rules
        append!(result, Integer(occursin(query, doc)))
    end
    return result
end

@everywhere function process_doc(file_path::AbstractString, rules::Array{Tuple{String, Regex}}, filter_mda_re::Union{Regex, Bool})::Array{Any}
    # Load file
    doc::String = read(file_path, String)

    # How big is the parsed file?
    doc_size::Integer = length(doc)

    # If the MD&A filter was given
    if typeof(filter_mda_re) == Regex
        mda_match::Union{RegexMatch, Nothing} = match(filter_mda_re, doc)
        if mda_match === nothing
            return []
        end
    end

    return [file_path; doc_size; check_rules(doc, rules)]
end

function main()
    n_threads = Threads.nthreads()
    println("[INFO] $n_threads threads ready.")
    rules = map(r -> (r[1], query_to_regex(r[2])), query_rules)

    name_of_loaded_rules = map(r -> r[1], rules)

    println("[INFO] Loaded rules: $name_of_loaded_rules")

    file_list = Glob.glob(TARGET_FILES)
    n_files = length(file_list)
    if n_files == 0
        println("[ERROR] No files were found in the given TARGET_FILES.")
        return 1
    end

    header = ["File path"; "Document size (b)"; name_of_loaded_rules]

    println("[INFO] Queueing $n_files parsing tasks in Julia cores.")

    filter_mda_re = filter_mda ? Regex("(?<=item 7)(.*)(?=item 8)", "si") : false

    return tmap(path -> process_doc(path, rules, filter_mda_re), file_list)

end

main()
