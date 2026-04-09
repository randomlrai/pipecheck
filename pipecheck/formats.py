"""Format detection and loading for DAG definitions."""

import json
import yaml
from pathlib import Path
from typing import Dict, Any
from .dag import DAG, Task


class FormatError(Exception):
    """Exception raised for format-related errors."""
    pass


class DAGLoader:
    """Loads DAG definitions from various file formats."""

    @staticmethod
    def load_from_file(filepath: str) -> DAG:
        """Load DAG from a file, auto-detecting format.
        
        Args:
            filepath: Path to the DAG definition file
            
        Returns:
            DAG object constructed from the file
            
        Raises:
            FormatError: If file format is unsupported or invalid
        """
        path = Path(filepath)
        
        if not path.exists():
            raise FormatError(f"File not found: {filepath}")
        
        suffix = path.suffix.lower()
        
        if suffix == '.json':
            return DAGLoader._load_from_json(path)
        elif suffix in ['.yml', '.yaml']:
            return DAGLoader._load_from_yaml(path)
        else:
            raise FormatError(f"Unsupported file format: {suffix}")

    @staticmethod
    def _load_from_json(path: Path) -> DAG:
        """Load DAG from JSON file."""
        try:
            with open(path, 'r') as f:
                data = json.load(f)
            return DAGLoader._build_dag_from_dict(data)
        except json.JSONDecodeError as e:
            raise FormatError(f"Invalid JSON: {e}")

    @staticmethod
    def _load_from_yaml(path: Path) -> DAG:
        """Load DAG from YAML file."""
        try:
            with open(path, 'r') as f:
                data = yaml.safe_load(f)
            return DAGLoader._build_dag_from_dict(data)
        except yaml.YAMLError as e:
            raise FormatError(f"Invalid YAML: {e}")

    @staticmethod
    def _build_dag_from_dict(data: Dict[str, Any]) -> DAG:
        """Build DAG object from dictionary representation.
        
        Args:
            data: Dictionary containing tasks list
            
        Returns:
            Constructed DAG object
        """
        if 'tasks' not in data:
            raise FormatError("Missing 'tasks' key in definition")
        
        dag = DAG()
        
        for task_def in data['tasks']:
            if 'id' not in task_def:
                raise FormatError("Task missing required 'id' field")
            
            task_id = task_def['id']
            dependencies = task_def.get('dependencies', [])
            metadata = task_def.get('metadata', {})
            
            task = Task(task_id, dependencies=dependencies, metadata=metadata)
            dag.add_task(task)
        
        return dag


if __name__ == "__main__":
    # Example usage
    pass
