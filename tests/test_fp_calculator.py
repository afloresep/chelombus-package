import sys
import os
import numpy as np

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import unittest
from unittest.mock import patch, MagicMock
from multiprocessing import Pool
from src.fingerprint_calculator import FingerprintCalculator, _calculate_fingerprint, init_worker
from config import N_JOBS

#from src.fingerprint_calculator import FingerprintCalculator
from rdkit import Chem

class TestFingerprintCalculator(unittest.TestCase):
    def setUp(self):
        """Set up the FingerprintCalculator instance for testing."""
        self.calculator = FingerprintCalculator()  # You can adjust n_jobs as needed

    def test_valid_smiles(self):
        """Test fingerprint calculation for valid SMILES strings."""
        smiles_list = [
            'C[C@H](N)C(=O)O',  # L-alanine
            'CC(C)C(C(=O)O)N',  # L-valine
            'CC(=O)OC1=CC=CC=C1C(=O)O',  # Aspirin
        ]
        fingerprints = self.calculator.calculate_fingerprints(smiles_list)
        self.assertEqual(len(fingerprints), len(smiles_list))
        for fp in fingerprints:
            self.assertIsNotNone(fp)
            self.assertIsInstance(fp, np.ndarray)  # Assuming fingerprint is a list; adjust as needed

    def test_invalid_smiles(self):
        """Test handling of invalid SMILES strings."""
        smiles_list = [
            'CCNC(=O)Nc1cn2c(-c3ncc(C)cn3)cc(-c3cncnc3)cc2n1',    # Valid
            'invalid_smiles',      # Invalid
            'COC(=O)[C@@H]1[C@@H]2C(=O)N(CC(=O)NC[C@H]3O[C@@H](n4ccc(=O)[nH]c4=O)[C@H](O)[C@@H]3O)C(=O)[C@@H]2N[C@@H]1c1ccccc1',  # Valid
            '',                    # Empty string (invalid)
            'C1CC1',               # Valid
            'C#Cc1ccc(-c2ccccc2)cc1',       # Valid
            'C1=CC=CC=C1C',        # Valid
            'C1=CC=CC=C',          # Invalid (incomplete ring)
        ]

        fingerprints = self.calculator.calculate_fingerprints(smiles_list)
        self.assertEqual(len(fingerprints), len(smiles_list))
        for smiles, fp in zip(smiles_list, fingerprints):
            if Chem.MolFromSmiles(smiles) is None:
                self.assertIsNone(fp)
            else:
                self.assertIsNotNone(fp)
                self.assertIsInstance(fp, np.ndarray)  

    def test_empty_input(self):
        """Test handling of empty input list."""
        smiles_list = []
        fingerprints = self.calculator.calculate_fingerprints(smiles_list)
        self.assertEqual(len(fingerprints), 0)
        self.assertEqual(fingerprints.tolist(), [])

    def test_single_smiles(self):
        """Test fingerprint calculation for a single SMILES string."""
        smiles_list = ['C1CCCCC1']  # Cyclohexane
        fingerprints = self.calculator.calculate_fingerprints(smiles_list)
        self.assertEqual(len(fingerprints), 1)
        self.assertIsNotNone(fingerprints[0])
        self.assertIsInstance(fingerprints[0], np.ndarray)  

    def test_large_dataset(self):
        """Test fingerprint calculation for a larger dataset."""
        smiles_list = ['C' * i for i in range(1, 100)]  # Generate simple SMILES strings
        fingerprints = self.calculator.calculate_fingerprints(smiles_list)
        self.assertEqual(len(fingerprints), len(smiles_list))
        for smiles, fp in zip(smiles_list, fingerprints):
            mol = Chem.MolFromSmiles(smiles)
            if mol is None:
                self.assertIsNone(fp)
            else:
                self.assertIsNotNone(fp)
                self.assertIsInstance(fp, np.ndarray)  

    def test_n_jobs(self):
        """Test fingerprint calculation with different n_jobs settings."""
        for n_jobs in [1, 2, 4]:
            with self.subTest(n_jobs=n_jobs):
                calculator = FingerprintCalculator()
                smiles_list = ['C[C@H](N)C(=O)O', 'CC(C)C(C(=O)O)N']
                fingerprints = calculator.calculate_fingerprints(smiles_list)
                self.assertEqual(len(fingerprints), len(smiles_list))
                for fp in fingerprints:
                    self.assertIsNotNone(fp)
                    self.assertIsInstance(fp, np.ndarray)  


if __name__ == '__main__':
    unittest.main()