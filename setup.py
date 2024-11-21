from setuptools import setup, find_packages

setup(
    name="chelombus",
    version="0.1.0",
    description="A package for fingerprints, PCA, and clustering of large molecular datasets.",
    author="Alejandro Flores S.",
    author_email="afloresep01@gmail.com",
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'calculate-fingerprints=scripts.fingerprint:main',
            'perform-pca=scripts.pca:main',
            'run-clustering=scripts.clustering:main',
        ],
    },
    install_requires=[
        "faerun>=0.4.7",
        "joblib>=1.4.2",
        "mapchiral>=0.0.6",
        "matplotlib>=3.9.2",
        "mhfp>=1.9.6",
        "numpy>=2.1.3",
        "pandarallel>=1.6.5",
        "pandas>=2.2.3",
        "pyspark>=3.5.3",
        "rdkit>=2024.3.6",
        "scikit-learn>=1.5.2",
        "tmap>=1.2.1",
        "tqdm>=4.66.5",
        "umap>=0.1.1",
    ],
    python_requires=">=3.7",
)
