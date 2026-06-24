input_file = r"C:/Users/Fotis Kalioras/Documents/GitHub/pycottas/experiments/DBPedia/dbpedia_fixed_rust.nt"
limit = 5_000_000
if limit > 1_000_000:
    output_file = f"dbpedia_{limit//1_000_000}M.nq"
elif limit > 1_000:
    output_file = f"dbpedia_{limit//1_000}K.nq"
else:
    output_file = f"dbpedia_{limit}.nq"

with open(input_file, "r", encoding="utf-8", errors="ignore") as infile, \
     open(output_file, "w", encoding="utf-8") as outfile:

    for i, line in enumerate(infile):
        if i >= limit:
            break

        line = line.strip()

        # Remove trailing " ." from N-Triples
        if line.endswith("."):
            line = line[:-1].strip()

        # Convert to N-Quads by adding a graph
        outfile.write(f"{line} <http://example.org/g0> .\n")

print(f"Done: wrote {limit} triples to {output_file}")

