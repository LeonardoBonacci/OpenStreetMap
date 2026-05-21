"""
Phase 5 — Text2Cypher

Natural-language questions → Cypher → answers, powered by a local
Ollama llama3.1:latest model via LangChain GraphCypherQAChain.

Usage:
    python src/text2cypher.py              # runs 5 demo questions
    python src/text2cypher.py --repl       # interactive prompt loop

Environment (.env):
    NEO4J_URI       bolt://localhost:7687
    NEO4J_USER      neo4j
    NEO4J_PASSWORD  <your password>
    OLLAMA_MODEL    llama3.1:latest          (optional override)
    OLLAMA_BASE_URL http://localhost:11434   (optional override)
"""

import os
import sys

from dotenv import load_dotenv
from langchain_core.prompts import PromptTemplate
from langchain_neo4j import GraphCypherQAChain, Neo4jGraph
from langchain_ollama import ChatOllama

load_dotenv()

NEO4J_URI       = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER      = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD  = os.getenv("NEO4J_PASSWORD", "")
OLLAMA_MODEL    = os.getenv("OLLAMA_MODEL", "llama3.1:latest")
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")

DEMO_QUESTIONS = [
    "Which road type has the most segments?",
    "What percentage of roads are one-way?",
    "Which intersection connects the most streets?",
    "Which named street has the longest total length?",
    "How many dead-end intersections are there?",
]


CYPHER_GENERATION_TEMPLATE = """You are a Neo4j Cypher expert. Generate a single valid Cypher query for the question below.

Schema:
{schema}

CRITICAL Neo4j Cypher rules (NOT SQL):
- There is NO GROUP BY in Cypher. Grouping happens automatically: non-aggregated keys are group-by keys.
  CORRECT: MATCH ()-[r:ROAD]->() RETURN r.highway AS highway, count(r) AS cnt ORDER BY cnt DESC LIMIT 1
  WRONG:   ... GROUP BY r.highway
- Percentage: use two separate WITH steps or toFloat():
  MATCH ()-[r:ROAD]->() WITH count(r) AS total MATCH ()-[r2:ROAD]->() WHERE r2.oneway = true WITH total, count(r2) AS oneway RETURN toFloat(oneway)/total*100 AS pct
- ROAD is a relationship, not a node. Pattern: (a:Intersection)-[r:ROAD]->(b:Intersection)
- Dead-end intersections: MATCH (i:Intersection) WHERE i.street_count = 1 RETURN count(i) AS cnt
- Longest named street: MATCH ()-[r:ROAD]->() WHERE r.name IS NOT NULL WITH r.name AS name, sum(r.length) AS total ORDER BY total DESC LIMIT 1 RETURN name, total
- Every expression in a WITH clause MUST be aliased (AS). Do NOT mix WITH aggregation and non-aggregated columns without aliasing all of them.
- Output ONLY the Cypher query — no markdown, no explanation, no semicolon.

Question: {question}
Cypher:"""

CYPHER_GENERATION_PROMPT = PromptTemplate(
    input_variables=["schema", "question"],
    template=CYPHER_GENERATION_TEMPLATE,
)

QA_TEMPLATE = """You are answering questions about a road network graph database.
Use ONLY the query results below to answer. State the answer directly and concisely.
If the results are empty, say "No data found."

Question: {question}
Query results: {context}
Answer:"""

QA_PROMPT = PromptTemplate(
    input_variables=["question", "context"],
    template=QA_TEMPLATE,
)


def build_chain(verbose: bool = True) -> GraphCypherQAChain:
    graph = Neo4jGraph(
        url=NEO4J_URI,
        username=NEO4J_USER,
        password=NEO4J_PASSWORD,
    )
    graph.refresh_schema()

    llm = ChatOllama(
        model=OLLAMA_MODEL,
        base_url=OLLAMA_BASE_URL,
        temperature=0,
    )

    return GraphCypherQAChain.from_llm(
        llm=llm,
        graph=graph,
        cypher_prompt=CYPHER_GENERATION_PROMPT,
        qa_prompt=QA_PROMPT,
        verbose=verbose,
        allow_dangerous_requests=True,
    )


def run_demo(chain: GraphCypherQAChain) -> None:
    print("\n=== Demo Questions ===\n")
    for q in DEMO_QUESTIONS:
        print(f"Q: {q}")
        try:
            result = chain.invoke({"query": q})
            print(f"A: {result['result']}\n")
        except Exception as exc:
            print(f"[ERROR] {exc}\n")


def run_repl(chain: GraphCypherQAChain) -> None:
    print("\nText2Cypher REPL — type 'exit' or Ctrl-C to quit.\n")
    while True:
        try:
            q = input("Q: ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if not q or q.lower() in {"exit", "quit"}:
            break
        try:
            result = chain.invoke({"query": q})
            print(f"A: {result['result']}\n")
        except Exception as exc:
            print(f"[ERROR] {exc}\n")


if __name__ == "__main__":
    chain = build_chain(verbose=True)
    if "--repl" in sys.argv:
        run_repl(chain)
    else:
        run_demo(chain)
