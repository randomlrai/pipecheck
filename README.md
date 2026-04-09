# pipecheck

A CLI tool for validating and visualizing data pipeline DAGs before deployment.

---

## Installation

```bash
pip install pipecheck
```

Or install from source:

```bash
git clone https://github.com/yourname/pipecheck.git && cd pipecheck && pip install -e .
```

---

## Usage

Point `pipecheck` at your pipeline definition file to validate its structure and catch issues before they reach production.

```bash
# Validate a pipeline DAG
pipecheck validate pipeline.yaml

# Visualize the DAG in your terminal
pipecheck visualize pipeline.yaml

# Validate and output a summary report
pipecheck validate pipeline.yaml --report report.json
```

**Example output:**

```
✔ No cycles detected
✔ All node dependencies resolved
⚠ Warning: Node "transform_sales" has no downstream consumers
✔ Pipeline is valid (8 nodes, 11 edges)
```

---

## Features

- Detects cycles and broken dependencies in pipeline graphs
- ASCII and Graphviz-based DAG visualization
- Supports YAML and JSON pipeline definitions
- Exit codes suitable for CI/CD integration

---

## License

This project is licensed under the [MIT License](LICENSE).