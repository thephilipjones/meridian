# Databricks notebook source
# MAGIC %pip install faker -q

# COMMAND ----------

"""Generate synthetic USPTO patent grant metadata for Meridian Regulatory."""

import json
import os
import random
import uuid
from datetime import datetime, timedelta

from faker import Faker

dbutils.widgets.text("catalog_name", "serverless_stable_k2zkdm_catalog")
_catalog = dbutils.widgets.get("catalog_name")

STAGING_PATH = f"/Volumes/{_catalog}/meridian_staging/patents"

fake = Faker()
Faker.seed(44)
random.seed(44)

NUM_PATENTS = 3000
RECORDS_PER_FILE = 500

# Shared company pool — overlaps with SEC and FDA generators for
# cross-source entity matching in the gold company_entities table.
COMPANIES = [
    "Pfizer Inc.", "Johnson & Johnson", "Merck & Co.", "AbbVie Inc.",
    "Bristol-Myers Squibb", "Amgen Inc.", "Gilead Sciences", "Eli Lilly",
    "Regeneron Pharmaceuticals", "Moderna Inc.", "Biogen Inc.",
    "Vertex Pharmaceuticals", "Illumina Inc.", "Thermo Fisher Scientific",
    "Danaher Corporation", "Becton Dickinson", "Edwards Lifesciences",
    "Intuitive Surgical", "Stryker Corporation", "Medtronic PLC",
    "Abbott Laboratories", "Baxter International", "Boston Scientific",
    "Zimmer Biomet", "Agilent Technologies", "Bio-Rad Laboratories",
    "Hologic Inc.", "ResMed Inc.", "West Pharmaceutical Services",
    "Catalent Inc.", "Exact Sciences", "Guardant Health",
    "Natera Inc.", "10x Genomics", "Twist Bioscience",
    "Novavax Inc.", "BioNTech SE", "CureVac NV", "Arctus Therapeutics",
    "Alnylam Pharmaceuticals", "Sarepta Therapeutics", "Blueprint Medicines",
    "Relay Therapeutics", "Revolution Medicines", "Arcus Biosciences",
    "Vir Biotechnology", "Krystal Biotech", "Disc Medicine",
    "Nuvalent Inc.", "Recursion Pharmaceuticals",
]

PATENT_TYPES = ["Utility", "Design", "Plant"]
PATENT_TYPE_WEIGHTS = [0.80, 0.15, 0.05]

USPC_CLASSES = {
    "424": "Drug, Bio-Affecting and Body Treating Compositions",
    "435": "Chemistry: Molecular Biology and Microbiology",
    "514": "Drug, Bio-Affecting and Body Treating Compositions (Organic)",
    "530": "Chemistry: Natural Resins or Derivatives; Peptides or Proteins",
    "536": "Organic Compounds — Carbohydrates, Nucleosides",
    "600": "Surgery — Diagnostic Testing",
    "604": "Surgery — Injection and Infusion Devices",
    "606": "Surgery — Instruments",
    "607": "Surgery — Light, Thermal, and Electrical Application",
    "623": "Prosthesis (Artificial Body Members)",
    "702": "Data Processing: Measuring, Calibrating, or Testing",
    "706": "Data Processing: Artificial Intelligence",
    "128": "Surgery — General",
    "351": "Optics: Eye Examining, Vision Testing and Correcting",
    "382": "Image Analysis",
}

TITLE_TEMPLATES = [
    "Compositions and methods for {action} {target}",
    "System and method for {process} in {application}",
    "{molecule_type} targeting {target} and uses thereof",
    "Devices and methods for {process}",
    "Modified {molecule_type} with enhanced {property}",
    "Method of {action} {target} using {technology}",
    "Formulation comprising {molecule_type} for {application}",
    "Apparatus for {process} with improved {property}",
    "Combination therapy comprising {molecule_type} and {second_agent}",
    "Biomarker panel for {application}",
]

_FILLERS = {
    "action": ["treating", "diagnosing", "modulating", "inhibiting", "detecting", "delivering"],
    "target": [
        "PD-1/PD-L1", "HER2", "KRAS G12C", "EGFR", "BCL-2", "CD19",
        "PCSK9", "IL-23", "TNF-alpha", "JAK1/2", "BRAF V600E", "RAS pathway",
    ],
    "process": [
        "nucleic acid sequencing", "cell sorting", "protein purification",
        "drug delivery", "tissue imaging", "gene editing", "biomarker detection",
    ],
    "application": [
        "oncology", "autoimmune disorders", "metabolic disease",
        "cardiovascular disease", "neurodegenerative disease", "infectious disease",
    ],
    "molecule_type": [
        "antibody", "siRNA", "mRNA", "peptide", "small molecule",
        "bispecific antibody", "ADC", "CAR-T cell", "nanoparticle",
    ],
    "property": [
        "bioavailability", "selectivity", "half-life", "specificity",
        "sensitivity", "throughput", "stability",
    ],
    "technology": [
        "CRISPR-Cas9", "lipid nanoparticles", "AAV vectors",
        "machine learning", "microfluidics", "next-generation sequencing",
    ],
    "second_agent": [
        "checkpoint inhibitor", "chemotherapy", "radiation therapy",
        "targeted kinase inhibitor", "anti-angiogenic agent",
    ],
}


def _gen_title() -> str:
    template = random.choice(TITLE_TEMPLATES)
    result = template
    for key, options in _FILLERS.items():
        placeholder = "{" + key + "}"
        if placeholder in result:
            result = result.replace(placeholder, random.choice(options), 1)
    return result


def _gen_abstract() -> str:
    sentences = [fake.paragraph(nb_sentences=2) for _ in range(3)]
    return " ".join(sentences)


def generate_patents(n: int) -> list[dict]:
    patents = []
    yesterday = datetime.today() - timedelta(days=1)
    base_date = yesterday - timedelta(days=3 * 365)
    span_days = (yesterday - base_date).days

    for i in range(n):
        assignee = random.choice(COMPANIES)
        filing_date = base_date + timedelta(days=random.randint(0, span_days))
        grant_lag = timedelta(days=random.randint(180, 900))
        grant_date = filing_date + grant_lag
        uspc = random.choice(list(USPC_CLASSES.keys()))

        patents.append({
            "patent_number": f"US{11000000 + i}",
            "title": _gen_title(),
            "abstract": _gen_abstract(),
            "assignee": assignee,
            "filing_date": filing_date.strftime("%Y-%m-%d"),
            "grant_date": grant_date.strftime("%Y-%m-%d"),
            "patent_type": random.choices(PATENT_TYPES, weights=PATENT_TYPE_WEIGHTS, k=1)[0],
            "uspc_class": uspc,
        })

    return patents


def write_json_files(patents: list[dict], output_path: str) -> list[str]:
    os.makedirs(output_path, exist_ok=True)
    filepaths = []

    for i in range(0, len(patents), RECORDS_PER_FILE):
        chunk = patents[i : i + RECORDS_PER_FILE]
        filename = f"patents_{i // RECORDS_PER_FILE:04d}.json"
        filepath = os.path.join(output_path, filename)
        with open(filepath, "w") as f:
            for record in chunk:
                f.write(json.dumps(record) + "\n")
        filepaths.append(filepath)

    return filepaths


def main(output_path: str | None = None):
    path = output_path or STAGING_PATH
    patents = generate_patents(NUM_PATENTS)
    filepaths = write_json_files(patents, path)
    print(f"Generated {len(patents)} patents across {len(filepaths)} files -> {path}")
    return patents


if __name__ == "__main__":
    main()
