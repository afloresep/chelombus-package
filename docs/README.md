## Installation

## **Table of Contents**

1. [Overview](#overview)
2. [Features](#features)
3. [Installation](#installation)
4. [Usage](#usage)
   - [Command-Line Interface (CLI)](#command-line-interface-cli)
   - [Using the Python Package](#using-the-python-package)
5. [Configuration](#configuration)
6. [Examples](#examples)
7. [API Reference](#api-reference)
8. [Project Structure](#project-structure)
9. [Testing](#testing)
10. [Contributing](#contributing)
11. [FAQ](#faq)
12. [License](#license)

---

## **1. Overview**

- **Project Name**: `Chelombus`
- **Description**: A Python package for processing large-scale datasets, including:
  - Fingerprint calculations for molecular data.
  - Dimensionality reduction using PCA.
  - Efficient clustering with Spark.
  - TMAP visualization for cluster representation.

- **Use Case**: Designed for molecular dataset analysis, particularly for large datasets (e.g., billions of molecules).

---

## **2. Features**

- **Modular Design**: Each component (fingerprint calculation, PCA, clustering) can be used independently.
- **Scalable**: Supports chunk-based processing and distributed clustering using Apache Spark.
- **Flexible Configuration**: Customize behavior through a YAML configuration file.
- **Extensibility**: Users can integrate new fingerprint methods, clustering algorithms, or visualizations.

---

## **3. Installation**

### **Requirements**

- Python 3.8 or higher
- Required libraries: `pandas`, `numpy`, `scikit-learn`, `pyspark`, `rdkit`, etc.

### **Steps**

1. Clone the repository:

   ```bash
   git clone https://github.com/yourusername/dynamic_tmap.git
   cd dynamic_tmap
   ```

2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Install the package:

   ```bash
   pip install .
   ```

After running `pip install .` you can:

- Import and use dynamic_tmap functionality programmatically:

```python
from dynamic_tmap.fingerprint_calculator import FingerprintCalculator
```

- Run the CLI commands directly:

```bash
calculate-fingerprints --input-file data.csv --output-dir fingerprints/
perform-pca --input-dir fingerprints/ --output-dir pca_results/
run-clustering --input-dir pca_results/ --output-dir clusters/
```

---

## **4. Usage**

### **Command-Line Interface (CLI)**

The package provides CLI tools for key functionalities:

- **Calculate Fingerprints**:

  ```bash
  python scripts/fingerprint.py --input-file data.csv --output-dir fingerprints/ --config user_config.yml
  ```

- **Perform PCA**:

  ```bash
  python scripts/pca.py --input-dir fingerprints/ --output-dir pca_results/ --config user_config.yml
  ```

- **Cluster Data**:

  ```bash
  python scripts/clustering.py --input-dir pca_results/ --output-dir clusters/ --config user_config.yml
  ```

### **Using the Python Package**

You can import and use individual modules programmatically:

```python
from dynamic_tmap.fingerprint_calculator import FingerprintCalculator
```

- from dynamic_tmap.dimensionality_reducer import DimensionalityReducer
from dynamic_tmap.clustering import ClusterMethod

# Example: Fingerprint Calculation

smiles = ["CCO", "CCCC", "C1=CC=CC=C1"]
fp_calculator = FingerprintCalculator(smiles, fingerprint_type="morgan", fp_size=1024)
fingerprints = fp_calculator.calculate_fingerprints()

---

## **5. Configuration**

### **Default Configuration**

The default configuration is defined in `config.py`. Below is an example `user_config.yml` file:

```yaml
DATA_FILE_PATH: "data/input.csv"
OUTPUT_FILE_PATH: "results/"
CHUNKSIZE: 100000
PCA_N_COMPONENTS: 3
STEPS_LIST: [50, 50, 50]
LOGGING_LEVEL: "INFO"
```

`config_loader.py` in `utils/` dynamically load and merges a user-specified configuration with the defaults.

You can update all scripts or modules that direclty import from config.py to instead use `load_config()`

```python
from utils.config_loader import load_config

# Load the configuration
config = load_config("path/to/user_config.yml")

# Example of usage
output_path = config["OUTPUT_FILE_PATH"]
chunksize = config["CHUNKSIZE"]
```

### Run Scripts with Custom Config

One can also pass the path to the user config file via CLI:

`python scripts/fingerprint.py --config user_config.yml`

> If no user config is provided, the defaults from `config.py`are used.

---

## **6. Examples**

### **Basic Workflow**

1. **Calculate Fingerprints**:

   ```bash
   python scripts/fingerprint.py --input-file data.csv --output-dir fingerprints/
   ```

2. **Perform PCA**:

   ```bash
   python scripts/pca.py --input-dir fingerprints/ --output-dir pca_results/
   ```

3. **Cluster Data**:

   ```bash
   python scripts/clustering.py --input-dir pca_results/ --output-dir clusters/
   ```

---

## **7. API Reference**

Document the key modules and their classes/functions with examples:

### **`dynamic_tmap.fingerprint_calculator`**

- **`FingerprintCalculator`**
  - `__init__(smiles_list, fingerprint_type, permutations=512, fp_size=1024, radius=2)`
  - `calculate_fingerprints()`

### **`dynamic_tmap.dimensionality_reducer`**

- **`DimensionalityReducer`**
  - `__init__(n_components, random_state=42)`
  - `fit(data)`
  - `transform(data)`

### **`dynamic_tmap.clustering`**

- **`ClusterMethod`**
  - `__init__(bin_size, output_path)`
  - `create_clusters(dataframe)`

---

## **8. Project Structure**

```plaintext
dynamic_tmap/
├── dynamic_tmap/
│   ├── __init__.py
│   ├── data_handler.py
│   ├── fingerprint_calculator.py
│   ├── clustering.py
│   ├── output_generator.py
│   ├── dimensionality_reducer.py
│   ├── utils/
│   │   ├── config_loader.py
│   │   ├── helper_functions.py
│   └── tests/
├── scripts/
│   ├── fingerprint.py
│   ├── pca.py
│   ├── clustering.py
│   └── run_pipeline.py
├── requirements.txt
├── setup.py
├── README.md
```

---

## **9. Testing**

- Run tests with:

  ```bash
  pytest tests/
  ```

- Example test for `FingerprintCalculator`:

  ```python
  from dynamic_tmap.fingerprint_calculator import FingerprintCalculator

  def test_fingerprint_calculator():
      smiles = ["CCO", "CCCC"]
      fp_calculator = FingerprintCalculator(smiles, fingerprint_type="morgan", fp_size=1024)
      fingerprints = fp_calculator.calculate_fingerprints()
      assert len(fingerprints) == len(smiles)
  ```

---

## **10. Contributing**

### **Guidelines**

1. Fork the repository.
2. Create a new branch:

   ```bash
   git checkout -b feature-name
   ```

3. Make changes and write tests.
4. Submit a pull request.

### **Code Style**

- Follow [PEP 8](https://www.python.org/dev/peps/pep-0008/).
- Use type annotations.

---

## **11. FAQ**

1. **How do I handle large datasets?**
   - Use the `CHUNKSIZE` configuration to process data in chunks.

2. **Can I use custom fingerprints?**
   - Extend `FingerprintCalculator` to add your own methods.

---

## **12. License**

Distributed under the MIT License. See `LICENSE` for details
---
