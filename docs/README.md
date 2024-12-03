## Installation

## **Table of Contents**

1. [Overview](#overview)
2. [Features](#features)
3. [Installation](#installation)
4. [Usage](#usage)
   - [Command-Line Interface (CLI)](#command-line-interface-cli)
   - [Using the Python Package](#using-the-python-package)
   - [Monitoring Utilities](#monitoring-utilities)
   - [CLI and API](#)
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

### Loading Data
Chelombus uses `DataHanlder.load_data` method for this. This function returns a generator object that will work with all file types supported (txt, csv, cxsmiles). 

All methods assume that the `smiles` values are on the first column and that the input data contains a header. Just as with pandas you can change this by updating `header=None` or `smiles_col_index=1` when creating a instance of the class for no header and smiles on column 1. This way only the smiles from the data is loaded to ensure speed and memory efficiency.  

#### Example Usage
Suppose your file looks like this (`input.txt`):

```
ID   SMILES           Property
1    CCCCCCCC         10.5
2    CCC=O            8.2
3    C1=CC=CC=C1      15.8
```

- If `smiles_col_index = 1` (zero-based index for SMILES):
  - `usecols=[1]` restricts the DataFrame to only the SMILES column.
  - Output for a chunk might look like:
    ```python
    ['CCCCCCCC', 'CCC=O', 'C1=CC=CC=C1']
    ```

Another example, if our `input.txt` has no header and our smiles values are on column 3, we'll do:

 `data_handler = Chelombus.DataHanlder(file_path = 'my_path', chunksize=1e7,header=None, smiles_col_index=2)`


### **Monitoring Utilities**
Chelombus includes utilities that allows you to monitor the execution time and memory usage of specific parts of your code using classes like `TimeTracker`, `RAMTracker` or both `RAMAndTimeTracker`.
<!-- TODO: COMPLETE  -->

### **CLI and API Integration**
**Chelombus** can be used through a **Command-Line Interface**, ideal for quick operations or automated workflows. 
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

The configuration (e.g. `input-path`, `output-dir`) can be passed as flags (_see `perform-pca --help` section for all the options available_) or through a configuration file provided by the user passing `--config my_config.env`
For more information about this, read the [Configuration](#5-configuration) documentation

Additionally, **chelombus** can also be used with a **Python API** to import into your Python scripts or projects, providing access to all functionality programmatically 
 
Examples:

```python
from dynamic_tmap.fingerprint_calculator import FingerprintCalculator
from dynamic_tmap.dimensionality_reducer import DimensionalityReducer
from dynamic_tmap.clustering import ClusterMethod

# Example: Fingerprint Calculation

smiles = ["CCO", "CCCC", "C1=CC=CC=C1"]
fp_calculator = FingerprintCalculator(smiles, fingerprint_type="morgan", fp_size=1024)
fingerprints = fp_calculator.calculate_fingerprints()
```

```python
from chelombus import pca

results = pca.run("my_datafile", output_dir="myfolder/")
print(results.summary())
```

---

## **5. Configuration**

Chelombus provides flexible configuration management using `pydantic`, allowing users to customize default settings efficiently. The default configuration values are defined in the `Config` class, which inherits from `BaseSettings`. Below is a list of the default values:

```python
BASE_DIR: str = os.getcwd()  # Base directory for project execution
DATA_PATH: str = "data/"  # Path for input data
OUTPUT_PATH: str = "data/output/"  # Path for output data
CHUNKSIZE: int = 100_000  # Size of data chunks for memory-intensive operations
IPCA_MODEL: str = None  # Path to a pre-trained IPCA model (if applicable)
PCA_N_COMPONENTS: int = 3  # Number of principal components for dimensionality reduction
STEPS_LIST: List[int] = [50, 50, 50]  # Number of buckets per PCA dimension
N_JOBS: int = os.cpu_count()  # Number of CPU cores to utilize (defaults to all cores)
RANDOM_STATE: int = 42  # Random seed for reproducibility
TMAP_NAME: str = "tmap"  # Name prefix for generated t-maps
PERMUTATIONS: int = 512  # Number of hashing permutations
TMAP_K: int = 20  # Number of neighbors considered in t-map
TMAP_NODE_SIZE: int = 5  # Size of nodes in the t-map visualization
TMAP_POINT_SCALE: float = 1.0  # Scale factor for t-map points
LOG_FILE_PATH: str = "logs/app.log"  # Log file path
LOGGING_LEVEL: str = "INFO"  # Logging verbosity level
LOGGING_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"  # Logging format
```

### **Modifying Configuration Values**

Chelombus allows users to customize the configuration in two ways:

---

#### **1. Changing Configuration Values via CLI**

You can override specific default values directly from the command line using flags. For example, to set a custom output directory (`OUTPUT_PATH`), use the corresponding flag, such as `--output-dir`. Only the specified value will be updated; all other settings will retain their defaults.

---

#### **2. Changing Configuration Values via an `.env` File**

Alternatively, you can use a configuration file by specifying its path with the `--config` flag. This allows you to define multiple settings in a single file instead of specifying each value via CLI flags.

When using a configuration file:
- The file should follow the `.env` format, which consists of key-value pairs.
- Any settings missing from the file will default to the values defined in the `Config` class.
- Use the same attribute names as in the `Config` class, prefixed with `CHELOMBUS_`.

For example, a `.env` file named `user_config.env` might look like this:

```env
CHELOMBUS_DATA_PATH=data/input.csv
CHELOMBUS_OUTPUT_PATH=results/
CHELOMBUS_CHUNKSIZE=200000
CHELOMBUS_PCA_N_COMPONENTS=5
CHELOMBUS_STEPS_LIST=[100, 200, 300]
CHELOMBUS_LOGGING_LEVEL=DEBUG
```

#### **Usage Example**

To load the configuration file, pass it to the `--config` flag:

```bash
perform-pca --config user_config.env
```

---

### **YAML Files and Compatibility**

Currently, Chelombus supports `.env` files for configuration. YAML files are not natively supported by `pydantic`. If you prefer YAML, consider converting the file to an `.env` format.

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
