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
            'calculate-fingerprints=cli.fingerprint:main',
            'perform-pca=cli.pca:main',
            'run-clustering=scripts.clustering:main',
        ],
    },
    install_requires=[
        "faerun>=0.4.7",
        "joblib>=1.3.2",
        "mapchiral>=0.0.6",
        "mhfp>=1.9.6",
        "numpy>=1.21.6",
        "pandarallel>=1.6.5",
        "pandas>=1.3.5",
        "rdkit>=2023.3.2",
        "scikit-learn>=1.0.2",
        "tmap>=1.2.1",
        "tqdm>=4.67.0",
    ],
    python_requires=">=3.7",
)
