"""
Test suite for FAC Data Integration Pipeline
"""
import unittest
import os
from pathlib import Path
import yaml
from run_pipeline import FACDataPipeline

class TestFACDataPipeline(unittest.TestCase):
    def setUp(self):
        """Set up test environment."""
        self.test_config = {
            'settings': {
                'output_filename': 'test_output.csv',
                'download_directory': 'test_downloads',
                'primary_join_key': 'report_id',
                'api_page_size': 10,
                'api_timeout_seconds': 30,
                'cleanup_temp_files': True
            },
            'sources': [
                {
                    'name': 'test_source',
                    'url': 'https://api.sam.gov/data-services/v3/fac/single_audits/general',
                    'api_params': {
                        'size': 10,
                        'from': 0
                    }
                }
            ]
        }
        self.config_path = 'test_config.yaml'
        with open(self.config_path, 'w') as f:
            yaml.dump(self.test_config, f)
            
    def tearDown(self):
        """Clean up test environment."""
        if os.path.exists(self.config_path):
            os.remove(self.config_path)
        if os.path.exists('test_downloads'):
            for file in Path('test_downloads').glob('*'):
                file.unlink()
            Path('test_downloads').rmdir()
            
    def test_config_loading(self):
        """Test configuration loading and validation."""
        pipeline = FACDataPipeline(self.config_path)
        self.assertEqual(pipeline.config['settings']['primary_join_key'], 'report_id')
        
    def test_config_validation(self):
        """Test configuration validation with missing required fields."""
        invalid_config = {
            'settings': {
                'output_filename': 'test.csv'
                # Missing required fields
            }
        }
        with open('invalid_config.yaml', 'w') as f:
            yaml.dump(invalid_config, f)
            
        with self.assertRaises(ValueError):
            FACDataPipeline('invalid_config.yaml')
            
        os.remove('invalid_config.yaml')
        
    def test_temp_directory_creation(self):
        """Test temporary directory creation."""
        pipeline = FACDataPipeline(self.config_path)
        self.assertTrue(os.path.exists(pipeline.temp_dir))
        
if __name__ == '__main__':
    unittest.main()
