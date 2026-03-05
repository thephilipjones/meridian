# Databricks notebook source
# MAGIC %pip install faker -q

# COMMAND ----------

"""Generate synthetic PubMed article metadata for Meridian Research."""

import json
import os
import random
import uuid
from datetime import datetime, timedelta

from faker import Faker

dbutils.widgets.text("catalog_name", "serverless_stable_k2zkdm_catalog")
_catalog = dbutils.widgets.get("catalog_name")

STAGING_PATH = f"/Volumes/{_catalog}/meridian_staging/pubmed"

fake = Faker()
Faker.seed(42)
random.seed(42)

NUM_ARTICLES = 5000
RECORDS_PER_FILE = 500

JOURNALS = [
    "Nature Medicine", "The Lancet", "JAMA", "The BMJ",
    "PLOS ONE", "Cell", "Science", "Nature Biotechnology",
    "Journal of Clinical Investigation", "Nucleic Acids Research",
    "Bioinformatics", "Genome Research", "Molecular Cell",
    "Nature Genetics", "Cancer Research", "Blood",
    "Journal of Immunology", "Neuron", "Circulation",
    "Gastroenterology", "Hepatology", "The New England Journal of Medicine",
    "Annals of Internal Medicine", "Nature Reviews Drug Discovery",
]

JOURNAL_WEIGHTS = [
    8, 7, 7, 6, 12, 5, 5, 4, 3, 6,
    5, 3, 3, 4, 5, 3, 4, 3, 3, 2, 2, 4, 3, 2,
]

MESH_TERMS = [
    "Humans", "Animals", "Male", "Female", "Adult", "Middle Aged",
    "CRISPR-Cas Systems", "Gene Editing", "Genome-Wide Association Study",
    "Machine Learning", "Deep Learning", "Artificial Intelligence",
    "Biomarkers", "Immunotherapy", "Drug Discovery", "Clinical Trial",
    "Neoplasms", "Breast Neoplasms", "Lung Neoplasms", "Colorectal Neoplasms",
    "Diabetes Mellitus", "Cardiovascular Diseases", "Alzheimer Disease",
    "COVID-19", "SARS-CoV-2", "Inflammation", "Apoptosis",
    "Signal Transduction", "Gene Expression Regulation",
    "Protein Binding", "Molecular Sequence Data",
    "RNA, Messenger", "MicroRNAs", "Epigenomics",
    "Stem Cells", "Cell Differentiation", "Cell Proliferation",
    "Antibodies, Monoclonal", "Receptors, Antigen, T-Cell",
    "Brain", "Liver", "Kidney", "Microbiome",
    "Drug Resistance", "Pharmacogenetics", "Proteomics", "Metabolomics",
    "Single-Cell Analysis", "Spatial Transcriptomics",
]

PUBLICATION_TYPES = [
    "Journal Article", "Journal Article", "Journal Article", "Journal Article",
    "Review", "Review",
    "Meta-Analysis",
    "Randomized Controlled Trial",
    "Clinical Trial", "Clinical Trial",
    "Observational Study",
    "Case Reports",
    "Comparative Study",
]

TOPIC_TEMPLATES = [
    "{approach} for {target} in {disease}",
    "A {study_type} of {target} {relationship} {disease}",
    "{approach} reveals {finding} in {tissue} {disease_context}",
    "{molecule} {action} {target} through {mechanism}",
    "Single-cell {approach} of {tissue} in {disease}",
    "Novel {molecule} targeting {target} for {disease} treatment",
    "{biomarker} as a prognostic marker in {disease}",
    "Genome-wide {approach} identifies {finding} in {disease}",
    "Machine learning-based {approach} for {disease} {outcome}",
    "The role of {pathway} in {disease} pathogenesis",
    "Efficacy and safety of {molecule} in {disease}: a {study_type}",
    "{approach} uncovers {mechanism} of {target} in {disease}",
    "Multi-omics {approach} of {tissue} reveals {finding}",
    "Long-term {outcome} of {molecule} therapy in {disease}",
    "Spatial transcriptomics of {tissue} in {disease_context}",
]

_FILLERS = {
    "approach": [
        "CRISPR screening", "Transcriptomic analysis", "Proteomic profiling",
        "Whole-genome sequencing", "Epigenomic mapping", "Deep learning",
        "Network analysis", "Metabolomic profiling", "Immunophenotyping",
        "High-throughput screening", "Structural analysis", "Functional genomics",
    ],
    "target": [
        "PD-L1", "BRCA1", "TP53", "EGFR", "HER2", "KRAS", "JAK2",
        "mTOR", "CDK4/6", "VEGF", "TNF-alpha", "IL-6", "TGF-beta",
        "PARP", "BCL-2", "PI3K", "checkpoint inhibitors",
    ],
    "disease": [
        "non-small cell lung cancer", "breast cancer", "colorectal cancer",
        "type 2 diabetes", "Alzheimer's disease", "Parkinson's disease",
        "rheumatoid arthritis", "systemic lupus erythematosus",
        "inflammatory bowel disease", "acute myeloid leukemia",
        "pancreatic ductal adenocarcinoma", "hepatocellular carcinoma",
        "chronic kidney disease", "heart failure", "major depressive disorder",
        "glioblastoma", "multiple myeloma", "melanoma",
    ],
    "study_type": [
        "randomized controlled trial", "meta-analysis", "cohort study",
        "systematic review", "phase III trial", "retrospective analysis",
        "multicenter study", "prospective observational study",
    ],
    "relationship": ["associated with", "as a predictor of", "in the progression of"],
    "finding": [
        "novel therapeutic targets", "driver mutations", "prognostic biomarkers",
        "drug resistance mechanisms", "cellular heterogeneity",
        "immune evasion pathways", "regulatory networks",
    ],
    "tissue": [
        "tumor microenvironment", "peripheral blood", "bone marrow",
        "liver tissue", "brain cortex", "gut epithelium", "pancreatic islets",
    ],
    "disease_context": [
        "during immunotherapy", "after chemoresistance", "at single-cell resolution",
        "across disease stages", "in treatment-naive patients",
    ],
    "molecule": [
        "pembrolizumab", "trastuzumab", "nivolumab", "osimertinib", "venetoclax",
        "ruxolitinib", "olaparib", "atezolizumab", "durvalumab", "sotorasib",
    ],
    "action": ["inhibits", "activates", "modulates", "downregulates", "stabilizes"],
    "mechanism": [
        "PI3K/AKT signaling", "NF-kB pathway", "Wnt/beta-catenin signaling",
        "autophagy", "epigenetic reprogramming", "ferroptosis",
    ],
    "pathway": [
        "JAK-STAT signaling", "MAPK cascade", "Notch pathway",
        "Hedgehog signaling", "TGF-beta/SMAD pathway", "Hippo pathway",
    ],
    "outcome": ["overall survival", "progression-free survival", "response rates", "remission"],
    "biomarker": [
        "Circulating tumor DNA", "Tumor mutational burden", "PD-L1 expression",
        "Microsatellite instability", "HbA1c levels", "Serum IL-6",
    ],
}


def _gen_title() -> str:
    template = random.choice(TOPIC_TEMPLATES)
    result = template
    for key, options in _FILLERS.items():
        placeholder = "{" + key + "}"
        if placeholder in result:
            result = result.replace(placeholder, random.choice(options), 1)
    return result


def _gen_abstract() -> str:
    sections = [
        f"BACKGROUND: {fake.paragraph(nb_sentences=3)}",
        f"METHODS: {fake.paragraph(nb_sentences=4)}",
        f"RESULTS: {fake.paragraph(nb_sentences=4)}",
        f"CONCLUSIONS: {fake.paragraph(nb_sentences=2)}",
    ]
    return " ".join(sections)


def _gen_authors(n: int) -> list[str]:
    return [f"{fake.last_name()}, {fake.first_name()}" for _ in range(n)]


def generate_articles(n: int) -> list[dict]:
    articles = []
    base_date = datetime(2023, 1, 1)

    for i in range(n):
        num_authors = random.choices([1, 2, 3, 4, 5, 6, 8, 12], weights=[3, 8, 15, 20, 20, 15, 10, 9], k=1)[0]
        num_mesh = random.randint(3, 12)
        num_pub_types = random.randint(1, 3)

        pub_date = base_date + timedelta(days=random.randint(0, 1050))
        pmid = str(30000000 + i)

        has_doi = random.random() < 0.92
        doi = f"10.{random.randint(1000, 9999)}/{fake.lexify('??????')}.{pub_date.year}.{random.randint(10000, 99999)}" if has_doi else None

        articles.append({
            "pmid": pmid,
            "doi": doi,
            "title": _gen_title(),
            "abstract": _gen_abstract(),
            "authors_raw": json.dumps(_gen_authors(num_authors)),
            "journal": random.choices(JOURNALS, weights=JOURNAL_WEIGHTS, k=1)[0],
            "publication_date": pub_date.strftime("%Y-%m-%d"),
            "mesh_terms_raw": json.dumps(random.sample(MESH_TERMS, min(num_mesh, len(MESH_TERMS)))),
            "publication_types": json.dumps(random.sample(PUBLICATION_TYPES, num_pub_types)),
            "source": "pubmed",
        })

    return articles


def write_json_files(articles: list[dict], output_path: str) -> list[str]:
    os.makedirs(output_path, exist_ok=True)
    filepaths = []

    for i in range(0, len(articles), RECORDS_PER_FILE):
        chunk = articles[i : i + RECORDS_PER_FILE]
        filename = f"pubmed_{i // RECORDS_PER_FILE:04d}.json"
        filepath = os.path.join(output_path, filename)
        with open(filepath, "w") as f:
            for article in chunk:
                f.write(json.dumps(article) + "\n")
        filepaths.append(filepath)

    return filepaths


def main(output_path: str | None = None):
    path = output_path or STAGING_PATH
    articles = generate_articles(NUM_ARTICLES)
    filepaths = write_json_files(articles, path)
    print(f"Generated {len(articles)} synthetic PubMed articles across {len(filepaths)} files -> {path}")
    return articles


if __name__ == "__main__":
    main()
